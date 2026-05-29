package parser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// TLVRecord is one (len, type, value) entry from the BLE advert. The
// admin debug view renders these to help an operator see why a packet
// did or didn't parse as an iBeacon.
type TLVRecord struct {
	Offset int    // byte offset in the advert where this record starts
	Length int    // value length (TLV length byte minus 1)
	Type   byte   // AD type byte (e.g. 0xFF for manufacturer-specific)
	Value  []byte // raw value bytes (length bytes)
	Label  string // best-effort human label (e.g. "Manufacturer-specific (Apple iBeacon)")
}

// DebugWalk is the result of a debug-mode parse.
type DebugWalk struct {
	Bytes   []byte      // decoded hex
	Records []TLVRecord // every TLV record in order
	IBeacon *IBeacon    // populated when an Apple iBeacon section was found
	Err     string      // non-empty when walking gave up midway
}

// ParseAdvertDebug is the diagnostic sibling of ParseAdvert. It walks
// every TLV record (not just the manuf-data ones) and annotates each
// with a human label. Errors mid-walk are returned in DebugWalk.Err
// rather than as a hard error so the operator still sees the records
// that parsed cleanly.
func ParseAdvertDebug(hexStr string) (DebugWalk, error) {
	var out DebugWalk
	if hexStr == "" {
		return out, nil
	}
	buf, err := hex.DecodeString(hexStr)
	if err != nil {
		return out, fmt.Errorf("hex decode: %w", err)
	}
	out.Bytes = buf
	for i := 0; i < len(buf); {
		recLen := int(buf[i])
		if recLen == 0 {
			break
		}
		if i+1+recLen > len(buf) {
			out.Err = fmt.Sprintf("TLV at offset %d: length %d runs past end of %d-byte buffer", i, recLen, len(buf))
			return out, nil
		}
		recType := buf[i+1]
		value := buf[i+2 : i+1+recLen]
		rec := TLVRecord{
			Offset: i,
			Length: recLen - 1,
			Type:   recType,
			Value:  append([]byte{}, value...),
			Label:  labelFor(recType, value),
		}
		out.Records = append(out.Records, rec)
		if recType == manufDataType && looksLikeIBeacon(value) {
			ib, derr := decodeIBeaconPayload(value)
			if derr == nil {
				cp := ib
				out.IBeacon = &cp
			} else {
				out.Err = derr.Error()
			}
		}
		i += 1 + recLen
	}
	return out, nil
}

func labelFor(t byte, v []byte) string {
	switch t {
	case 0x01:
		return "Flags"
	case 0x02, 0x03:
		return "Incomplete/Complete list of 16-bit Service UUIDs"
	case 0x06, 0x07:
		return "Incomplete/Complete list of 128-bit Service UUIDs"
	case 0x08, 0x09:
		return "Shortened/Complete Local Name"
	case 0x0A:
		return "Tx Power Level"
	case 0x16:
		return "Service Data"
	case manufDataType:
		if looksLikeIBeacon(v) {
			return "Manufacturer-specific (Apple iBeacon)"
		}
		if len(v) >= 2 {
			cid := binary.LittleEndian.Uint16(v[:2])
			return fmt.Sprintf("Manufacturer-specific (company 0x%04X)", cid)
		}
		return "Manufacturer-specific"
	default:
		return fmt.Sprintf("AD type 0x%02X", t)
	}
}
