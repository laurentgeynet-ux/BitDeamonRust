// golden-vectors generator — dumps wire-format fixtures produced by the
// *C++* implementation, consumed by BitDaemon conformance tests.
//
// Build (against the upstream checkout, e.g. D:\Projet\libtorrent):
//
//   cmake -S . -B build -DCMAKE_PREFIX_PATH=<libtorrent-install>
//   or compile directly:
//   c++ -std=c++17 generate.cpp -I<upstream>/include -o generate \
//       -L<upstream>/build -ltorrent
//
// Each fixture lands in OUT_DIR as `<name>.bin` (+ a `manifest.txt`
// listing them). Add generators here as Phase F conformance tests need
// them; keep names stable — the Rust side loads them by name.

#include <libtorrent/bencode.hpp>
#include <libtorrent/bdecode.hpp>
#include <libtorrent/entry.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/write_resume_data.hpp>
#include <libtorrent/read_resume_data.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/create_torrent.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/aux_/merkle.hpp>
#include <libtorrent/fingerprint.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/kademlia/dht_state.hpp>
#include <libtorrent/kademlia/node_entry.hpp>
#include <libtorrent/hex.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/torrent_flags.hpp>
#include <libtorrent/storage_defs.hpp>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace lt;

static void dump(fs::path const& out, std::string const& name,
	std::vector<char> const& data)
{
	std::ofstream f(out / (name + ".bin"), std::ios::binary);
	f.write(data.data(), static_cast<std::streamsize>(data.size()));
	std::printf("wrote %s (%zd bytes)\n", name.c_str(), data.size());
}

static void dump_entry(fs::path const& out, std::string const& name,
	entry const& e)
{
	std::vector<char> buf;
	bencode(std::back_inserter(buf), e);
	dump(out, name, buf);
}

int main(int argc, char** argv)
{
	fs::path const out = argc > 1 ? argv[1] : fs::current_path();
	fs::create_directories(out);

	// --- bencode fixtures -------------------------------------------
	// canonical dict order, nested containers, negative ints, binary
	{
		entry d(entry::dictionary_t);
		d["b"] = entry("x");
		d["a"] = entry(42);
		d["c"] = entry(entry::list_t);
		d["c"].list().emplace_back(entry::string_t);
		dump_entry(out, "bencode_sorted_dict", d);

		entry neg(entry::dictionary_t);
		neg["n"] = entry(-9223372036854775807LL - 1);
		neg["empty"] = entry(std::string());
		dump_entry(out, "bencode_edge_ints", neg);
	}

	// --- resume data -------------------------------------------------
	// write_resume_data output over a synthetic AddTorrentParams is the
	// golden blob BitDaemon must produce byte-for-byte.
	{
		add_torrent_params p;
		p.name = "fixture";
		// p.ti left null: write_resume_data emits the metadata-less
		// form — still exercises trackers/flags/priorities fields.
		p.trackers = {{"http://t1/announce"}, {"http://t2/announce"}};
		p.tracker_tiers = {0, 1};
		p.file_priorities = {lt::download_priority_t{4}, lt::dont_download};
		dump_entry(out, "resume_empty", write_resume_data(p));
		dump_entry(out, "resume_buf_empty", write_resume_data_buf(p));
	}

	// --- resume data, rich -------------------------------------------
	// seed_mode + counters + limits + flags: exercises the flag-heavy
	// branches of write_resume_data.
	{
		add_torrent_params p;
		p.name = "rich";
		p.flags = torrent_flags::seed_mode | torrent_flags::auto_managed
			| torrent_flags::sequential_download | torrent_flags::disable_dht;
		p.total_uploaded = 1024;
		p.total_downloaded = 2048;
		p.active_time = 60;
		p.seeding_time = 120;
		p.finished_time = 30;
		p.last_seen_complete = 7;
		p.added_time = 1'600'000'000;
		p.completed_time = 1'600'000'100;
		p.upload_limit = 5000;
		p.download_limit = 8000;
		p.max_connections = 50;
		p.max_uploads = 12;
		p.num_complete = 42;
		p.num_incomplete = 13;
		p.num_downloaded = 9;
		p.save_path = "C:/downloads";
		p.storage_mode = lt::storage_mode_allocate;
		dump_entry(out, "resume_rich", write_resume_data(p));
	}

	// --- fingerprint --------------------------------------------------
	// pins the "-XXNNNN-" client-id prefix encoding.
	{
		std::string fp = generate_fingerprint("BD", 0, 1, 0, 0);
		dump(out, "fingerprint_bd0100",
			std::vector<char>(fp.begin(), fp.end()));
		std::string fp2 = generate_fingerprint("LT", 2, 1, 2, 0);
		dump(out, "fingerprint_lt2120",
			std::vector<char>(fp2.begin(), fp2.end()));
	}

	// --- magnet_uri -----------------------------------------------------
	// make_magnet_uri output pins ordering/escaping of xt/xl/dn/tr/x.pe.
	{
		add_torrent_params p;
		p.name = "fixture";
		p.info_hashes.v1 = sha1_hash("01234567890123456789");
		p.info_hashes.v2 = sha256_hash("01234567890123456789012345678901");
		p.trackers = {"http://t1/announce", "udp://t2:443/announce"};
		p.url_seeds = {"http://ws/files/"};
		std::string const uri = make_magnet_uri(p);
		dump(out, "magnet_hybrid", std::vector<char>(uri.begin(), uri.end()));
	}

	// --- session params (.session file) ---------------------------------
	// write_session_params output is the format the .session file uses;
	// contains dht_state + settings_pack defaults. Deterministic because
	// session_params() starts empty.
	{
		session_params sp;
		dump_entry(out, "session_default", write_session_params(sp));

		session_params sp2;
		sp2.settings.set_int(settings_pack::download_rate_limit, 12345);
		sp2.settings.set_bool(settings_pack::enable_dht, false);
		sp2.settings.set_str(settings_pack::user_agent, "fixture/1.0");
		dump_entry(out, "session_custom", write_session_params(sp2));
	}

	// --- dht_state -------------------------------------------------------
	// save_dht_state pins the n4/n6 node-list serialization.
	{
		dht::dht_state st;
		st.nids.emplace_back(make_address_v4("1.2.3.4"), sha1_hash("01234567890123456789"));
		st.nids.emplace_back(make_address_v4("5.6.7.8"), sha1_hash("abcdefghijklmnopqrst"));
		st.nodes.emplace_back(make_address_v4("9.10.11.12"), 6881);
		dump_entry(out, "dht_state_nodes", dht::save_dht_state(st));
	}

	// --- create_torrent / merkle ------------------------------------
	// small synthetic FileStorage → v1 + v2 .torrent files; the info
	// dict section must hash identically under BitDaemon.
	{
		file_storage fstore;
		char const payload[] = "hello world fixture payload";
		fs::path const tmp = fs::temp_directory_path() / "gv_seed";
		fs::create_directories(tmp);
		{
			std::ofstream f(tmp / "a.txt", std::ios::binary);
			f.write(payload, sizeof(payload) - 1);
		}
		add_files(fstore, (tmp / "a.txt").string());
		create_torrent ct(fstore);
		ct.add_tracker("http://t/announce");
		set_piece_hashes(ct, tmp.string());
		dump_entry(out, "torrent_v1_synthetic", ct.generate());
	}

	// --- v2-only torrent -------------------------------------------------
	// BEP-52: merkle trees per file + meta-version 2 info dict.
	{
		file_storage fstore;
		char const payload[] = "v2 fixture payload bytes!";
		auto const tmp = fs::temp_directory_path() / "gv_seed_v2";
		fs::create_directories(tmp);
		{
			std::ofstream f(tmp / "b.bin", std::ios::binary);
			f.write(payload, sizeof(payload) - 1);
		}
		add_files(fstore, (tmp / "b.bin").string());
		create_torrent ct(fstore, 16 * 1024, create_torrent::v2_only);
		ct.add_tracker("http://t/announce");
		set_piece_hashes(ct, tmp.string());
		dump_entry(out, "torrent_v2_synthetic", ct.generate());
	}

	std::printf("done -> %s\n", out.string().c_str());
	return 0;
}
