# 3-minute walkthrough: Kafka Streaming Pipeline

A timed script for a Loom recording of about 3 minutes (screen plus camera bubble, 1080p). Say it in your own words; the lines are a guide, not a teleprompter.

**Open these tabs before you press record:** README, the pipeline diagram, `stream/pipeline.py`, `stream/windows.py`, `stream/alerts.py`, `output/run_summary.md`, `tests/test_windows.py`, GitHub Actions.

| Time | On screen | What to say |
|---|---|---|
| 0:00–0:20 | Portfolio card, then the README title | Real event streams have duplicates, late data and broken messages, and naive pipelines publish wrong numbers. This one computes on-time delivery per depot from parcel events, and alerts on real problems only. |
| 0:20–0:40 | README intro | A simulator produces six hours of events across four depots, about 30,000 messages, with a planted incident at one depot. Near-real-time monitoring was part of my Bose work; my Device Telemetry project is the larger streaming example. |
| 0:40–1:05 | Pipeline diagram, then `pipeline.py` | The core is broker-agnostic: the Kafka consumer and the simulator feed the same processor, so all the logic is tested without a broker. |
| 1:05–1:35 | `windows.py` | Five-minute windows on event time, not arrival time. A watermark with ten minutes' allowed lateness decides when a window is final; anything later goes to a dead-letter topic rather than silently changing published numbers. |
| 1:35–2:05 | README: delivery guarantees | At-least-once delivery with idempotent processing: offsets commit after processing, duplicates drop by event id, and the DuckDB sink upserts, so replays don't double count. |
| 2:05–2:30 | `output/run_summary.md` | 24,616 messages counted, 868 duplicates ignored, 295 late and 128 invalid sent to the DLQ. All 18 alerts fired at the incident depot inside the incident window, with no false alarms elsewhere. |
| 2:30–2:45 | `alerts.py` | An earlier version with a 10-point threshold on thin windows fired false alarms at healthy depots. Skipping thin windows and comparing each depot to its own baseline fixed it, and the README says why. |
| 2:45–3:00 | Back to the README top | The Kafka wrappers are thin and haven't run against a live broker yet; CI covers the core. For an ops team, that's alerts they can act on. Links are under the video. |

## Recording checklist
- Close email, chat and notifications. Zoom the browser to 125% so code is readable.
- Run `python -m stream.simulate` once so the run summary is fresh. Don't demo the live Kafka wrappers; they haven't run against a broker yet.
- Keep to one take per section; Loom lets you trim. Aim for 2:45 to 3:10.
- Show real numbers on screen as you say them; never estimate on camera.
- Say "synthetic" out loud when you introduce the data. This is a skills demonstration, not a rebuild of client work.

## After recording
1. Copy the Loom share link.
2. In `README.md`, replace the walkthrough line with `- [x] [3-minute Loom walkthrough](<your Loom link>)` and add the link under the title.
3. Add the link to this project's card on saigwe.github.io.
