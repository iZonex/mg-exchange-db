//! GeoHash encoding/decoding utilities.
//!
//! Uses the standard interleaving algorithm where longitude bits occupy even
//! positions and latitude bits occupy odd positions within the hash.

/// Encode a latitude/longitude pair into a geohash with the given precision
/// (number of bits per axis, so total bits = 2 * precision, max 32 for i64).
///
/// `precision` is the number of bits per axis (1..=32). The resulting hash
/// uses `2 * precision` bits total, stored in the low bits of an `i64`.
pub fn encode_geohash(lat: f64, lon: f64, precision: u8) -> i64 {
    let precision = precision.min(32) as u32;
    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;
    let mut min_lon = -180.0_f64;
    let mut max_lon = 180.0_f64;

    let mut hash: i64 = 0;

    for i in 0..precision {
        // Longitude bit (even position from the top)
        let mid_lon = (min_lon + max_lon) / 2.0;
        let lon_bit = if lon >= mid_lon {
            min_lon = mid_lon;
            1_i64
        } else {
            max_lon = mid_lon;
            0_i64
        };

        // Latitude bit (odd position from the top)
        let mid_lat = (min_lat + max_lat) / 2.0;
        let lat_bit = if lat >= mid_lat {
            min_lat = mid_lat;
            1_i64
        } else {
            max_lat = mid_lat;
            0_i64
        };

        let bit_pos = (precision - 1 - i) * 2;
        hash |= lon_bit << (bit_pos + 1);
        hash |= lat_bit << bit_pos;
    }

    hash
}

/// Decode a geohash back to the center latitude/longitude of its cell.
///
/// `precision` must match the precision used during encoding.
pub fn decode_geohash(hash: i64, precision: u8) -> (f64, f64) {
    let precision = precision.min(32) as u32;
    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;
    let mut min_lon = -180.0_f64;
    let mut max_lon = 180.0_f64;

    for i in 0..precision {
        let bit_pos = (precision - 1 - i) * 2;

        // Longitude bit
        let lon_bit = (hash >> (bit_pos + 1)) & 1;
        let mid_lon = (min_lon + max_lon) / 2.0;
        if lon_bit == 1 {
            min_lon = mid_lon;
        } else {
            max_lon = mid_lon;
        }

        // Latitude bit
        let lat_bit = (hash >> bit_pos) & 1;
        let mid_lat = (min_lat + max_lat) / 2.0;
        if lat_bit == 1 {
            min_lat = mid_lat;
        } else {
            max_lat = mid_lat;
        }
    }

    ((min_lat + max_lat) / 2.0, (min_lon + max_lon) / 2.0)
}

/// Return the geohashes of the 8 neighboring cells (N, NE, E, SE, S, SW, W, NW).
///
/// Neighbors are computed by adjusting the lat/lon index by +/-1 and
/// re-interleaving. If a neighbor would exceed the valid coordinate range
/// it wraps around (longitude) or is clamped (latitude).
pub fn geohash_neighbors(hash: i64, precision: u8) -> Vec<i64> {
    let precision = precision.min(32) as u32;

    // De-interleave into separate lat_idx and lon_idx.
    let mut lat_idx: i64 = 0;
    let mut lon_idx: i64 = 0;
    for i in 0..precision {
        let bit_pos = i * 2;
        lat_idx |= ((hash >> bit_pos) & 1) << i;
        lon_idx |= ((hash >> (bit_pos + 1)) & 1) << i;
    }

    let max_val = 1_i64 << precision; // number of cells per axis

    let offsets: [(i64, i64); 8] = [
        (1, 0),   // N
        (1, 1),   // NE
        (0, 1),   // E
        (-1, 1),  // SE
        (-1, 0),  // S
        (-1, -1), // SW
        (0, -1),  // W
        (1, -1),  // NW
    ];

    let mut neighbors = Vec::with_capacity(8);
    for (dlat, dlon) in &offsets {
        let nlat = lat_idx + dlat;
        let nlon = lon_idx + dlon;

        // Clamp latitude, wrap longitude
        if nlat < 0 || nlat >= max_val {
            continue; // skip neighbors that go past the poles
        }
        let nlon = ((nlon % max_val) + max_val) % max_val;

        // Re-interleave
        let mut h: i64 = 0;
        for i in 0..precision {
            h |= ((nlon >> i) & 1) << (i * 2 + 1);
            h |= ((nlat >> i) & 1) << (i * 2);
        }
        neighbors.push(h);
    }

    neighbors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let lat = 48.8566; // Paris
        let lon = 2.3522;
        for precision in [8, 16, 20, 25, 32] {
            let hash = encode_geohash(lat, lon, precision);
            let (dlat, dlon) = decode_geohash(hash, precision);
            // Error should halve with each additional bit of precision.
            let max_err_lat = 180.0 / (1u64 << precision) as f64;
            let max_err_lon = 360.0 / (1u64 << precision) as f64;
            assert!(
                (dlat - lat).abs() <= max_err_lat,
                "lat error too large at precision {precision}: {} vs {lat}",
                dlat
            );
            assert!(
                (dlon - lon).abs() <= max_err_lon,
                "lon error too large at precision {precision}: {} vs {lon}",
                dlon
            );
        }
    }

    #[test]
    fn encode_decode_various_locations() {
        let locations = [
            (0.0, 0.0),           // null island
            (90.0, 0.0),          // north pole
            (-90.0, 0.0),         // south pole
            (0.0, 180.0),         // date line
            (0.0, -180.0),        // date line
            (51.5074, -0.1278),   // London
            (-33.8688, 151.2093), // Sydney
        ];
        let precision = 25;
        for (lat, lon) in &locations {
            let hash = encode_geohash(*lat, *lon, precision);
            let (dlat, dlon) = decode_geohash(hash, precision);
            let max_err = 180.0 / (1u64 << precision) as f64;
            assert!(
                (dlat - lat).abs() <= max_err,
                "lat mismatch for ({lat}, {lon})"
            );
            assert!(
                (dlon - lon).abs() <= 2.0 * max_err,
                "lon mismatch for ({lat}, {lon})"
            );
        }
    }

    #[test]
    fn neighbors_count() {
        // Interior cell should have 8 neighbors
        let hash = encode_geohash(48.8566, 2.3522, 16);
        let nbrs = geohash_neighbors(hash, 16);
        assert_eq!(nbrs.len(), 8);
        // All neighbors should be distinct
        let mut sorted = nbrs.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 8);
    }

    #[test]
    fn precision_levels() {
        let lat = 40.7128;
        let lon = -74.0060;
        let hash_low = encode_geohash(lat, lon, 5);
        let hash_high = encode_geohash(lat, lon, 20);
        // Higher precision should use more bits
        assert!(hash_high > hash_low || hash_high != hash_low);
        // Low precision decode should have larger error
        let (dlat_low, _) = decode_geohash(hash_low, 5);
        let (dlat_high, _) = decode_geohash(hash_high, 20);
        assert!((dlat_high - lat).abs() <= (dlat_low - lat).abs() + 1e-10);
    }
}
