// lt_client — minimal libtorrent C++ client for differential
// real-swarm testing against BitDaemon.
//
// Modes:
//   make  <payload_dir_or_file> <out.torrent> — create a v1 .torrent
//   makessl <payload_dir_or_file> <out.torrent> <root_ca.pem>
//         — SSL torrent: embeds the root CA PEM in the info dict
//   info  <file.torrent> — print name + v1/v2 info-hashes
//   seed  <file.torrent> <save_path> <port> [seconds] — seed
//   sslseed <file.torrent> <save_path> <port> <seconds> <cert> <key> <dh>
//         — seed on an ssl: listen interface
//   leech <file.torrent> <save_path> <port> <ip:port> [peer2…]
//         — download, exit when seeding, print stats; keeps
//           <file.torrent>.resume fresh and resumes from it if present
//   utpleech …— leech with outbound/inbound TCP disabled (uTP-only)
//   sslleech <file.torrent> <save_path> <port> <ip:port> <cert> <key> <dh>
//         — download over TLS peer connections
//
// Build: see build_lt_client.bat

#include <libtorrent/session.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/create_torrent.hpp>
#include <libtorrent/hex.hpp>
#include <libtorrent/load_torrent.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/torrent_flags.hpp>

#include <filesystem>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace lt;
using namespace std::chrono;

static int make_torrent(char const* payload, char const* out,
    create_flags_t flags)
{
    file_storage fstore;
    add_files(fstore, payload);
    if (fstore.num_files() == 0) { std::fprintf(stderr, "no files\n"); return 1; }
    create_torrent ct(fstore, 256 * 1024, -1, flags);
    std::filesystem::path const pp = std::filesystem::path(payload).parent_path();
    set_piece_hashes(ct, pp.empty() ? "." : pp.string());
    std::ofstream f(out, std::ios::binary);
    bencode(std::ostream_iterator<char>(f), ct.generate());
    std::printf("made %s (%d files, %d pieces)\n", out, fstore.num_files(), fstore.num_pieces());
    return 0;
}

static int make_ssl_torrent(char const* payload, char const* out, char const* root_ca)
{
    file_storage fstore;
    add_files(fstore, payload);
    if (fstore.num_files() == 0) { std::fprintf(stderr, "no files\n"); return 1; }
    create_torrent ct(fstore, 256 * 1024);
    std::filesystem::path const pp = std::filesystem::path(payload).parent_path();
    set_piece_hashes(ct, pp.empty() ? "." : pp.string());
    std::ifstream caf(root_ca, std::ios::binary);
    std::string ca_pem((std::istreambuf_iterator<char>(caf)), std::istreambuf_iterator<char>());
    if (ca_pem.empty()) { std::fprintf(stderr, "empty ca\n"); return 1; }
    ct.set_root_cert(ca_pem);
    std::ofstream f(out, std::ios::binary);
    bencode(std::ostream_iterator<char>(f), ct.generate());
    std::printf("made %s (%d files, %d pieces, ssl-cert embedded)\n",
        out, fstore.num_files(), fstore.num_pieces());
    return 0;
}

static session_params base_params(int port)
{
    settings_pack sp;
    sp.set_int(settings_pack::alert_mask, alert::all_categories);
    sp.set_str(settings_pack::listen_interfaces, "0.0.0.0:" + std::to_string(port));
    // keep the comparison fair: disable services BitDaemon also lacks
    // or that add noise on loopback
    sp.set_bool(settings_pack::enable_upnp, false);
    sp.set_bool(settings_pack::enable_natpmp, false);
    sp.set_bool(settings_pack::enable_lsd, false);
    sp.set_bool(settings_pack::enable_dht, false);
    // the whole bench runs on 127.0.0.1 — without this upstream drops
    // same-IP connections as duplicates (uTP+TCP races, multi-client
    // PEX topologies)
    sp.set_bool(settings_pack::allow_multiple_connections_per_ip, true);
    session_params p(sp);
    return p;
}

// IPv6 loopback listen — `[::1]:port` (upstream listen_interfaces
// v6 syntax); exercises the v6 wire path against BitDaemon.
static session_params v6_params(int port)
{
    settings_pack sp;
    sp.set_int(settings_pack::alert_mask, alert::all_categories);
    sp.set_str(settings_pack::listen_interfaces, "[::1]:" + std::to_string(port));
    sp.set_bool(settings_pack::enable_upnp, false);
    sp.set_bool(settings_pack::enable_natpmp, false);
    sp.set_bool(settings_pack::enable_lsd, false);
    sp.set_bool(settings_pack::enable_dht, false);
    sp.set_bool(settings_pack::allow_multiple_connections_per_ip, true);
    return session_params(sp);
}

static int v6seed(char const* torr, char const* save, int port, int seconds)
{
    auto atp = load_torrent_file(torr);
    session ses(v6_params(port));
    atp.save_path = save;
    atp.flags |= torrent_flags::seed_mode;
    ses.async_add_torrent(std::move(atp));
    std::printf("v6 seeding on [::1]:%d for %ds\n", port, seconds);
    auto end = steady_clock::now() + std::chrono::seconds(seconds);
    while (steady_clock::now() < end)
    {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        ses.post_torrent_updates();
        std::vector<alert*> alerts;
        ses.pop_alerts(&alerts);
        for (alert* a : alerts)
            std::printf("  seed alert[%d]: %s\n", a->type(), a->message().c_str());
    }
    return 0;
}

static int v6leech(char const* torr, char const* save, int port,
    std::vector<std::string> const& peers)
{
    auto atp = load_torrent_file(torr);
    session ses(v6_params(port));
    atp.save_path = save;
    auto h = ses.add_torrent(std::move(atp));
    for (auto const& p : peers)
    {
        auto pos = p.rfind(':');
        if (pos == std::string::npos) continue;
        auto host = p.substr(0, pos);
        if (host.size() > 2 && host.front() == '[' && host.back() == ']')
            host = host.substr(1, host.size() - 2);
        error_code ec;
        auto ip = make_address(host.c_str(), ec);
        if (ec) continue;
        h.connect_peer(tcp::endpoint(ip, (unsigned short)std::atoi(p.c_str() + pos + 1)));
    }
    auto t0 = steady_clock::now();
    long long last_done = -1;
    for (;;)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ses.post_torrent_updates();
        std::vector<alert*> alerts;
        ses.pop_alerts(&alerts);
        for (alert* a : alerts)
        {
            std::printf("  leech alert[%d]: %s\n", a->type(), a->message().c_str());
            if (auto* st = alert_cast<state_update_alert>(a))
                for (auto const& s : st->status)
                    if (s.total_done != last_done)
                    {
                        last_done = s.total_done;
                        std::printf("  leech: %lld/%lld peers=%d\n",
                            (long long)s.total_done, (long long)s.total_wanted,
                            s.num_peers);
                    }
        }
        torrent_status st = h.status();
        if (st.is_seeding) { std::printf("DONE\n"); return 0; }
        if (duration_cast<seconds>(steady_clock::now() - t0).count() > 300)
            return 2;
    }
}

// MSE/PE: force encrypted peer connections, RC4-only — exercises the
// full DH-768 + RC4 cipher path against BitDaemon (`pe_forced` both
// directions + `pe_rc4` level so plaintext-select can't hide a bug).
static void apply_enc(session_params& p)
{
    p.settings.set_int(settings_pack::out_enc_policy, settings_pack::pe_forced);
    p.settings.set_int(settings_pack::in_enc_policy, settings_pack::pe_forced);
    p.settings.set_int(settings_pack::allowed_enc_level, settings_pack::pe_rc4);
    p.settings.set_bool(settings_pack::prefer_rc4, true);
}

static int seed(char const* torr, char const* save, int port, int seconds,
    bool enc = false, char const* dht_node = nullptr, bool lsd = false,
    bool sseed = false, bool uponly = false)
{
    auto atp = load_torrent_file(torr);
    auto params = base_params(port);
    if (enc) apply_enc(params);
    if (dht_node)
    {
        params.settings.set_bool(settings_pack::enable_dht, true);
        params.settings.set_str(settings_pack::dht_bootstrap_nodes, dht_node);
    }
    if (lsd)
    {
        params.settings.set_bool(settings_pack::enable_lsd, true);
        std::printf("lsd mode\n");
    }
    session ses(params);
    atp.save_path = save;
    atp.flags |= torrent_flags::seed_mode;
    if (sseed)
    {
        atp.flags |= torrent_flags::super_seeding;
        std::printf("super-seeding\n");
    }
    if (uponly)
    {
        atp.flags |= torrent_flags::upload_mode;
        std::printf("upload-only\n");
    }
    ses.async_add_torrent(std::move(atp));
    std::printf("seeding on port %d for %ds\n", port, seconds);
    auto end = steady_clock::now() + std::chrono::seconds(seconds);
    while (steady_clock::now() < end)
    {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        ses.post_torrent_updates();
        std::vector<alert*> alerts;
        ses.pop_alerts(&alerts);
        for (alert* a : alerts)
        {
            std::printf("  seed alert[%d]: %s\n", a->type(), a->message().c_str());
            if (auto* st = alert_cast<state_update_alert>(a))
                for (auto const& s : st->status)
                    std::printf("  seed: up=%lld peers=%d\n",
                        (long long)s.total_upload, s.num_peers);
        }
        for (auto const& th : ses.get_torrents())
        {
            std::vector<peer_info> pi;
            th.get_peer_info(pi);
            for (auto const& p : pi)
                std::printf("  seed peer %s flags=%x uo=%d\n",
                    p.ip.address().to_string().c_str(),
                    static_cast<unsigned int>(p.flags),
                    bool(p.flags & peer_info::upload_only));
        }
    }
    return 0;
}

static int info(char const* torr)
{
    auto atp = load_torrent_file(torr);
    auto const& ih = atp.ti->info_hashes();
    std::printf("name: %s\n", atp.ti->name().c_str());
    if (ih.has_v1()) std::printf("v1: %s\n", aux::to_hex(ih.v1).c_str());
    if (ih.has_v2()) std::printf("v2: %s\n", aux::to_hex(ih.v2).c_str());
    return 0;
}

static int leech(char const* torr, char const* save, int port,
    std::vector<std::string> const& peers, bool utp_only, bool enc = false,
    char const* dht_node = nullptr, bool lsd = false)
{
    auto atp = load_torrent_file(torr);

    // cross-client resume: if a previous run (either client) left a
    // `<torrent>.resume` file, continue from it
    std::string const rpath = std::string(torr) + ".resume";
    if (std::filesystem::exists(rpath))
    {
        std::ifstream rf(rpath, std::ios::binary);
        std::vector<char> rbuf((std::istreambuf_iterator<char>(rf)),
            std::istreambuf_iterator<char>());
        error_code ec;
        auto ratp = read_resume_data(rbuf, ec);
        if (!ec)
        {
            if (!ratp.ti) ratp.ti = atp.ti;
            ratp.save_path = save;
            atp = std::move(ratp);
            std::printf("resumed from %s\n", rpath.c_str());
        }
        else std::fprintf(stderr, "bad resume: %s\n", ec.message().c_str());
    }

    auto params = base_params(port);
    if (enc) apply_enc(params);
    if (dht_node)
    {
        params.settings.set_bool(settings_pack::enable_dht, true);
        params.settings.set_str(settings_pack::dht_bootstrap_nodes, dht_node);
    }
    if (lsd)
    {
        params.settings.set_bool(settings_pack::enable_lsd, true);
        std::printf("lsd mode\n");
    }
    if (utp_only)
    {
        params.settings.set_bool(settings_pack::enable_outgoing_tcp, false);
        params.settings.set_bool(settings_pack::enable_incoming_tcp, false);
        params.settings.set_bool(settings_pack::enable_outgoing_utp, true);
        params.settings.set_bool(settings_pack::enable_incoming_utp, true);
        std::printf("utp-only mode (TCP disabled)\n");
    }
    session ses(params);
    atp.save_path = save;
    auto h = ses.add_torrent(std::move(atp));

    for (auto const& p : peers)
    {
        auto pos = p.rfind(':');
        if (pos == std::string::npos) continue;
        error_code ec;
        auto ip = make_address(p.substr(0, pos).c_str(), ec);
        if (ec) continue;
        h.connect_peer(tcp::endpoint(ip, (unsigned short)std::atoi(p.c_str() + pos + 1)));
    }

    auto t0 = steady_clock::now();
    long long last_done = -1;
    int iters = 0;
    for (;;)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (++iters % 8 == 0)
        {
            std::vector<peer_info> pi;
            h.get_peer_info(pi);
            for (auto const& p : pi)
                std::printf("  leech peer %s flags=%x uo=%d\n",
                    p.ip.address().to_string().c_str(),
                    static_cast<unsigned int>(p.flags),
                    bool(p.flags & peer_info::upload_only));
        }
        // keep `<torrent>.resume` fresh so a kill mid-download leaves
        // usable resume state (cross-client interop test)
        h.save_resume_data(torrent_handle::save_info_dict);
        std::vector<alert*> alerts;
        ses.pop_alerts(&alerts);
        for (alert* a : alerts)
        {
            std::printf("  seed alert[%d]: %s\n", a->type(), a->message().c_str());
            if (auto* sr = alert_cast<save_resume_data_alert>(a))
            {
                auto const buf = write_resume_data_buf(sr->params);
                std::ofstream rf(rpath, std::ios::binary);
                rf.write(buf.data(), (std::streamsize)buf.size());
            }
            if (auto* st = alert_cast<state_update_alert>(a))
            {
                for (auto const& s : st->status)
                {
                    if (s.total_done != last_done)
                    {
                        last_done = s.total_done;
                        auto ms = duration_cast<milliseconds>(steady_clock::now() - t0).count();
                        std::printf("  leech: done=%lld/%lld peers=%d dl=%d t=%lldms\n",
                            (long long)s.total_done, (long long)s.total_wanted,
                            s.num_peers, s.download_rate, (long long)ms);
                    }
                }
            }
            if (alert_cast<torrent_finished_alert>(a) || alert_cast<torrent_checked_alert>(a)) {}
        }
        auto s = h.status();
        if (s.is_seeding || s.is_finished)
        {
            auto ms = duration_cast<milliseconds>(steady_clock::now() - t0).count();
            std::printf("DONE pieces=%d bytes=%lld peers_seen=%d time=%lldms\n",
                s.num_pieces, (long long)s.total_done, s.list_peers, (long long)ms);
            return 0;
        }
        if (duration_cast<seconds>(steady_clock::now() - t0).count() > 300)
        {
            std::printf("TIMEOUT done=%lld/%lld\n", (long long)s.total_done, (long long)s.total_wanted);
            return 2;
        }
    }
}

// SSL variants: `…s` listen interface + per-torrent keypair
// (`set_ssl_certificate` upstream). `cert`/`key`/`dh` are PEM paths.
static session_params ssl_params(int port, bool ssl_listen)
{
    settings_pack sp;
    sp.set_int(settings_pack::alert_mask, alert::all_categories);
    std::string ifaces = "0.0.0.0:" + std::to_string(port);
    if (ssl_listen) ifaces += ",0.0.0.0:" + std::to_string(port + 1) + "s";
    sp.set_str(settings_pack::listen_interfaces, ifaces);
    sp.set_bool(settings_pack::enable_upnp, false);
    sp.set_bool(settings_pack::enable_natpmp, false);
    sp.set_bool(settings_pack::enable_lsd, false);
    sp.set_bool(settings_pack::enable_dht, false);
    sp.set_bool(settings_pack::allow_multiple_connections_per_ip, true);
    return session_params(sp);
}

static void set_cert(torrent_handle& h, char const* cert, char const* key, char const* dh)
{
    h.set_ssl_certificate(cert, key, dh, "test");
}

static int sslseed(char const* torr, char const* save, int port, int seconds,
    char const* cert, char const* key, char const* dh)
{
    auto atp = load_torrent_file(torr);
    session ses(ssl_params(port, true));
    atp.save_path = save;
    atp.flags |= torrent_flags::seed_mode;
    auto h = ses.add_torrent(std::move(atp));
    set_cert(h, cert, key, dh);
    std::printf("ssl seeding on port %d (ssl:%d) for %ds\n", port, port + 1, seconds);
    auto end = steady_clock::now() + std::chrono::seconds(seconds);
    while (steady_clock::now() < end)
    {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        ses.post_torrent_updates();
        std::vector<alert*> alerts;
        ses.pop_alerts(&alerts);
        for (alert* a : alerts)
        {
            std::printf("  seed alert[%d]: %s\n", a->type(), a->message().c_str());
            if (auto* st = alert_cast<state_update_alert>(a))
                for (auto const& s : st->status)
                    std::printf("  seed: up=%lld peers=%d\n",
                        (long long)s.total_upload, s.num_peers);
        }
    }
    return 0;
}

static int sslleech(char const* torr, char const* save, int port,
    char const* peer, char const* cert, char const* key, char const* dh,
    bool utp_only = false)
{
    auto atp = load_torrent_file(torr);
    auto params = ssl_params(port, true);
    if (utp_only)
    {
        params.settings.set_bool(settings_pack::enable_outgoing_tcp, false);
        params.settings.set_bool(settings_pack::enable_incoming_tcp, false);
        params.settings.set_bool(settings_pack::enable_outgoing_utp, true);
        params.settings.set_bool(settings_pack::enable_incoming_utp, true);
        std::printf("ssl utp-only mode (TCP disabled)\n");
    }
    session ses(params);
    atp.save_path = save;
    auto h = ses.add_torrent(std::move(atp));
    set_cert(h, cert, key, dh);
    auto pos = std::string(peer).rfind(':');
    error_code ec;
    auto ip = make_address(std::string(peer).substr(0, pos).c_str(), ec);
    if (ec) { std::fprintf(stderr, "bad peer\n"); return 1; }
    h.connect_peer(tcp::endpoint(ip,
        (unsigned short)std::atoi(peer + pos + 1)));
    auto t0 = steady_clock::now();
    long long last_done = -1;
    for (;;)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::vector<alert*> alerts;
        ses.pop_alerts(&alerts);
        for (alert* a : alerts)
        {
            std::printf("  leech alert[%d]: %s\n", a->type(), a->message().c_str());
            if (auto* st = alert_cast<state_update_alert>(a))
                for (auto const& s : st->status)
                    if (s.total_done != last_done)
                    {
                        last_done = s.total_done;
                        std::printf("  leech: done=%lld/%lld peers=%d dl=%d\n",
                            (long long)s.total_done, (long long)s.total_wanted,
                            s.num_peers, s.download_rate);
                    }
        }
        auto s = h.status();
        if (s.is_seeding || s.is_finished)
        {
            std::printf("DONE pieces=%d bytes=%lld\n",
                s.num_pieces, (long long)s.total_done);
            return 0;
        }
        if (duration_cast<seconds>(steady_clock::now() - t0).count() > 300)
        {
            std::printf("TIMEOUT done=%lld/%lld\n",
                (long long)s.total_done, (long long)s.total_wanted);
            return 2;
        }
    }
}

int main(int argc, char** argv)
{
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if (argc < 2) { std::fprintf(stderr, "usage: lt_client make|makev1|makev2|makessl|info|seed|sslseed|leech|utpleech|sslleech …\n"); return 1; }
    std::string mode = argv[1];
    try
    {
        if (mode == "make" && argc == 4)
            return make_torrent(argv[2], argv[3], create_torrent::merkle);
        if (mode == "makev1" && argc == 4)
            return make_torrent(argv[2], argv[3], create_torrent::v1_only);
        if (mode == "makev2" && argc == 4)
            return make_torrent(argv[2], argv[3], create_torrent::v2_only);
        if (mode == "makessl" && argc == 5) return make_ssl_torrent(argv[2], argv[3], argv[4]);
        if (mode == "sslseed" && argc >= 8)
            return sslseed(argv[2], argv[3], std::atoi(argv[4]),
                std::atoi(argv[5]), argv[6], argv[7], argv[8]);
        if ((mode == "sslleech" || mode == "sslutpleech") && argc >= 9)
            return sslleech(argv[2], argv[3], std::atoi(argv[4]),
                argv[5], argv[6], argv[7], argv[8], mode == "sslutpleech");
        if (mode == "seed" && argc >= 5)
            return seed(argv[2], argv[3], std::atoi(argv[4]), argc > 5 ? std::atoi(argv[5]) : 600);
        if (mode == "sseed" && argc >= 5)
            return seed(argv[2], argv[3], std::atoi(argv[4]), argc > 5 ? std::atoi(argv[5]) : 600,
                false, nullptr, false, true);
        if (mode == "upseed" && argc >= 5)
            return seed(argv[2], argv[3], std::atoi(argv[4]), argc > 5 ? std::atoi(argv[5]) : 600,
                false, nullptr, false, false, true);
        if (mode == "encseed" && argc >= 5)
            return seed(argv[2], argv[3], std::atoi(argv[4]), argc > 5 ? std::atoi(argv[5]) : 600, true);
        if (mode == "dhtseed" && argc >= 7)
            return seed(argv[2], argv[3], std::atoi(argv[4]),
                std::atoi(argv[5]), false, argv[6]);
        if (mode == "dhtleech" && argc >= 6)
        {
            std::vector<std::string> peers;
            for (int i = 6; i < argc; ++i) peers.emplace_back(argv[i]);
            return leech(argv[2], argv[3], std::atoi(argv[4]), peers,
                false, false, argv[5]);
        }
        if (mode == "v6seed" && argc >= 6)
            return v6seed(argv[2], argv[3], std::atoi(argv[4]), std::atoi(argv[5]));
        if (mode == "v6leech" && argc >= 5)
        {
            std::vector<std::string> peers;
            for (int i = 5; i < argc; ++i) peers.emplace_back(argv[i]);
            return v6leech(argv[2], argv[3], std::atoi(argv[4]), peers);
        }
        if (mode == "lsdseed" && argc >= 6)
            return seed(argv[2], argv[3], std::atoi(argv[4]),
                std::atoi(argv[5]), false, nullptr, true);
        if (mode == "lsdleech" && argc >= 5)
        {
            std::vector<std::string> peers;
            for (int i = 5; i < argc; ++i) peers.emplace_back(argv[i]);
            return leech(argv[2], argv[3], std::atoi(argv[4]), peers,
                false, false, nullptr, true);
        }
        if (mode == "info" && argc == 3) return info(argv[2]);
        if ((mode == "leech" || mode == "utpleech" || mode == "encleech") && argc >= 5)
        {
            std::vector<std::string> peers;
            for (int i = 5; i < argc; ++i) peers.emplace_back(argv[i]);
            return leech(argv[2], argv[3], std::atoi(argv[4]), peers,
                mode == "utpleech", mode == "encleech");
        }
    }
    catch (std::exception const& e) { std::fprintf(stderr, "error: %s\n", e.what()); return 1; }
    std::fprintf(stderr, "bad args\n");
    return 1;
}
