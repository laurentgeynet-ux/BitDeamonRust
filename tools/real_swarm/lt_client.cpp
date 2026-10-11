// lt_client — minimal libtorrent C++ client for differential
// real-swarm testing against BitDaemon.
//
// Modes:
//   make  <payload_dir_or_file> <out.torrent> — create a v1 .torrent
//   seed  <file.torrent> <save_path> <port> [seconds] — seed
//   leech <file.torrent> <save_path> <port> <ip:port> [peer2…]
//         — download, exit when seeding, print stats
//
// Build: see build_lt_client.bat

#include <libtorrent/session.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/create_torrent.hpp>
#include <libtorrent/load_torrent.hpp>
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

static int make_torrent(char const* payload, char const* out)
{
    file_storage fstore;
    add_files(fstore, payload);
    if (fstore.num_files() == 0) { std::fprintf(stderr, "no files\n"); return 1; }
    create_torrent ct(fstore, 256 * 1024);
    std::filesystem::path const pp = std::filesystem::path(payload).parent_path();
    set_piece_hashes(ct, pp.empty() ? "." : pp.string());
    std::ofstream f(out, std::ios::binary);
    bencode(std::ostream_iterator<char>(f), ct.generate());
    std::printf("made %s (%d files, %d pieces)\n", out, fstore.num_files(), fstore.num_pieces());
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
    session_params p(sp);
    return p;
}

static int seed(char const* torr, char const* save, int port, int seconds)
{
    auto atp = load_torrent_file(torr);
    session ses(base_params(port));
    atp.save_path = save;
    atp.flags |= torrent_flags::seed_mode;
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
    }
    return 0;
}

static int leech(char const* torr, char const* save, int port,
    std::vector<std::string> const& peers)
{
    auto atp = load_torrent_file(torr);
    session ses(base_params(port));
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
    for (;;)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::vector<alert*> alerts;
        ses.pop_alerts(&alerts);
        for (alert* a : alerts)
        {
            std::printf("  seed alert[%d]: %s\n", a->type(), a->message().c_str());
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

int main(int argc, char** argv)
{
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if (argc < 2) { std::fprintf(stderr, "usage: lt_client make|seed|leech …\n"); return 1; }
    std::string mode = argv[1];
    try
    {
        if (mode == "make" && argc == 4) return make_torrent(argv[2], argv[3]);
        if (mode == "seed" && argc >= 5)
            return seed(argv[2], argv[3], std::atoi(argv[4]), argc > 5 ? std::atoi(argv[5]) : 600);
        if (mode == "leech" && argc >= 5)
        {
            std::vector<std::string> peers;
            for (int i = 5; i < argc; ++i) peers.emplace_back(argv[i]);
            return leech(argv[2], argv[3], std::atoi(argv[4]), peers);
        }
    }
    catch (std::exception const& e) { std::fprintf(stderr, "error: %s\n", e.what()); return 1; }
    std::fprintf(stderr, "bad args\n");
    return 1;
}
