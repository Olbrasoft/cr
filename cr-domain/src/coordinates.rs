//! Coordinates value object with latitude/longitude validation.

/// Geographic coordinates with validated latitude and longitude.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coordinates {
    latitude: f64,
    longitude: f64,
}

impl Coordinates {
    /// Create new coordinates with validation.
    ///
    /// Returns `None` if latitude is outside -90..=90 or longitude is outside -180..=180.
    pub fn new(latitude: f64, longitude: f64) -> Option<Self> {
        if !(-90.0..=90.0).contains(&latitude) || !(-180.0..=180.0).contains(&longitude) {
            return None;
        }
        Some(Self {
            latitude,
            longitude,
        })
    }

    pub fn latitude(&self) -> f64 {
        self.latitude
    }

    pub fn longitude(&self) -> f64 {
        self.longitude
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_coordinates() {
        let c = Coordinates::new(49.8, 15.5).unwrap();
        assert_eq!(c.latitude(), 49.8);
        assert_eq!(c.longitude(), 15.5);
    }

    #[test]
    fn boundary_values() {
        assert!(Coordinates::new(90.0, 180.0).is_some());
        assert!(Coordinates::new(-90.0, -180.0).is_some());
        assert!(Coordinates::new(0.0, 0.0).is_some());
    }

    #[test]
    fn invalid_latitude() {
        assert!(Coordinates::new(91.0, 15.0).is_none());
        assert!(Coordinates::new(-91.0, 15.0).is_none());
    }

    #[test]
    fn invalid_longitude() {
        assert!(Coordinates::new(49.0, 181.0).is_none());
        assert!(Coordinates::new(49.0, -181.0).is_none());
    }
}
