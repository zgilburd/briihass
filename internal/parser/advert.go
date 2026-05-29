// Package parser walks BLE advertisement TLVs and extracts iBeacons.
//
// See ADR-0001 (verified iBeacon plugin payload) and the ruckus-apis skill
// for the parsing recipe.
package parser

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

// IBeacon is a parsed Apple iBeacon advertisement.
type IBeacon struct {
	UUID    string // canonical lowercase 8-4-4-4-12 form
	Major   uint16
	Minor   uint16
	TxPower int8 // calibrated TX power at 1m, dBm (signed int8)
}

// Apple's company identifier (little-endian on the wire) and the iBeacon
// marker bytes that follow it inside a manuf-data section.
const (
	appleCompanyIDLE = 0x004C // bytes 0x4C 0x00
	iBeaconType      = 0x02
	iBeaconLength    = 0x15 // 21 bytes: UUID(16)+Major(2)+Minor(2)+TX(1)
	manufDataType    = 0xFF
)

// ParseAdvert decodes a hex-encoded BLE advertisement and walks its TLV
// structure looking for a manufacturer-specific section whose value begins
// with Apple's company ID followed by the iBeacon marker bytes.
//
// Returns:
//   - (ib, true,  nil)   if an iBeacon was found
//   - (zero, false, nil) if the advert parses correctly but is not an
//     iBeacon (Apple Continuity, AirDrop, AltBeacon, random BLE, etc.)
//   - (zero, false, err) if the hex is malformed or a TLV length runs
//     past the end of the buffer
//
// Empty input returns (zero, false, nil) — common for missing or empty
// "data" fields in upstream payloads.
func ParseAdvert(hexStr string) (IBeacon, bool, error) {
	if hexStr == "" {
		return IBeacon{}, false, nil
	}
	buf, err := hex.DecodeString(hexStr)
	if err != nil {
		return IBeacon{}, false, fmt.Errorf("hex decode: %w", err)
	}

	for i := 0; i < len(buf); {
		// Each TLV record: <len> <type> <value (len-1 bytes)>.
		recLen := int(buf[i])
		if recLen == 0 {
			// Per Bluetooth spec, a zero length terminates the AD list.
			break
		}
		if i+1+recLen > len(buf) {
			return IBeacon{}, false, fmt.Errorf("TLV at offset %d: length %d runs past end of %d-byte buffer", i, recLen, len(buf))
		}
		recType := buf[i+1]
		value := buf[i+2 : i+1+recLen] // (recLen - 1) bytes

		if recType == manufDataType && looksLikeIBeacon(value) {
			ib, err := decodeIBeaconPayload(value)
			if err != nil {
				return IBeacon{}, false, err
			}
			return ib, true, nil
		}
		i += 1 + recLen
	}
	return IBeacon{}, false, nil
}

// looksLikeIBeacon reports whether a manuf-data value starts with the
// 4-byte signature: Apple company ID (0x4C 0x00, little-endian) +
// iBeacon type byte (0x02) + length byte (0x15).
//
// The exactly-25-bytes requirement is enforced in decodeIBeaconPayload.
func looksLikeIBeacon(v []byte) bool {
	return len(v) >= 4 &&
		v[0] == 0x4C && v[1] == 0x00 &&
		v[2] == iBeaconType && v[3] == iBeaconLength
}

func decodeIBeaconPayload(v []byte) (IBeacon, error) {
	// Layout after the 4-byte signature:
	//   UUID    : 16 bytes  (offset 4..19)
	//   Major   :  2 bytes  (offset 20..21, big-endian)
	//   Minor   :  2 bytes  (offset 22..23, big-endian)
	//   TX power:  1 byte   (offset 24, signed)
	const want = 4 + 16 + 2 + 2 + 1
	if len(v) < want {
		return IBeacon{}, fmt.Errorf("iBeacon payload: want %d bytes, got %d", want, len(v))
	}
	uuidBytes := v[4:20]
	major := binary.BigEndian.Uint16(v[20:22])
	minor := binary.BigEndian.Uint16(v[22:24])
	tx := int8(v[24])
	return IBeacon{
		UUID:    formatUUID(uuidBytes),
		Major:   major,
		Minor:   minor,
		TxPower: tx,
	}, nil
}

func formatUUID(b []byte) string {
	if len(b) != 16 {
		// Should be unreachable given the caller's bounds check.
		return ""
	}
	const hexDigits = "0123456789abcdef"
	// 8-4-4-4-12 = 32 hex chars + 4 dashes = 36
	out := make([]byte, 36)
	pos := 0
	emitGroup := func(start, end int) {
		for k := start; k < end; k++ {
			out[pos] = hexDigits[b[k]>>4]
			out[pos+1] = hexDigits[b[k]&0x0F]
			pos += 2
		}
	}
	emitGroup(0, 4)
	out[pos] = '-'
	pos++
	emitGroup(4, 6)
	out[pos] = '-'
	pos++
	emitGroup(6, 8)
	out[pos] = '-'
	pos++
	emitGroup(8, 10)
	out[pos] = '-'
	pos++
	emitGroup(10, 16)
	return string(out)
}

// ErrIncompletePayload is returned when an iBeacon signature is present
// but the trailing payload is short. Tests can use errors.Is against it.
var ErrIncompletePayload = errors.New("iBeacon payload truncated")
