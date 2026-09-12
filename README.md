# LD2410 Calibrator

A single-file, phone-driven tool that calibrates the HLK-LD2410 presence
radar behind an ESPHome device. It talks to the device only through the
ESPHome built-in web server (REST plus server-sent events), so no extra
firmware, broker, or API key is needed.

![The calibration page on a phone, showing the live move and still energy of
each gate against its current threshold](screenshot.png)

## What it does

1. Connects to the device and turns engineering mode on, so per-gate
   energies are published.
2. Measures the empty room for 120 s.
3. Measures you walking from the sensor to the far wall for 60 s.
4. Measures you sitting still for 60 s.
5. Proposes a move and still threshold per gate, a max distance gate from
   your room length, and a timeout.
6. Writes them to the radar and verifies each value echoed back.
7. Turns engineering mode off on every exit path.

The page shows live gate energies against the current thresholds while you
measure, so you can see what the radar sees.

## Requirements

- [uv](https://docs.astral.sh/uv/) on the machine that runs the script.
- An ESPHome device with the `ld2410` component, `web_server:` enabled, and
  these entities exposed with names ending in the strings below:
  - switch: `Engineering Mode`
  - numbers: `Timeout`, `Max Move Distance Gate`, `Max Still Distance Gate`,
    and for each gate 0 to 8: `Gate N Move Threshold`, `Gate N Still Threshold`
  - sensors for each gate 0 to 8: `Gate N Move Energy`, `Gate N Still Energy`
- The phone and the device on the same LAN, and port 8765 open on the
  machine running the script.

## Usage

```sh
./calibrate_ld2410.py
```

Open the printed URL on your phone, enter the device hostname and the room
length from the sensor to the far wall, and follow the instructions.


## License

AGPL-3.0. See `LICENSE`.
