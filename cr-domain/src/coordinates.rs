//! Coordinates value object with latitude/longitude validation.

use crate::error::DomainError;

/// Geographic coordinates with validated latitude and longitude.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coordinates {
    latitude: f64,
    longitude: f64,
}

impl Coordinates {
    /// Create new coordinates with validation.
    ///
    /// Returns `Err(DomainError)` if latitude is outside -90..=90 or longitude is outside -180..=180.
    pub fn new(latitude: f64, longitude: f64) -> Result<Self, DomainError> {
        if !(-90.0..=90.0).contains(&latitude) {
            return Err(DomainError::InvalidLatitude(latitude));
        }
        if !(-180.0..=180.0).contains(&longitude) {
            return Err(DomainError::InvalidLongitude(longitude));
        }
        Ok(Self {
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
        assert!(Coordinates::new(90.0, 180.0).is_ok());
        assert!(Coordinates::new(-90.0, -180.0).is_ok());
        assert!(Coordinates::new(0.0, 0.0).is_ok());
    }

    #[test]
    fn invalid_latitude() {
        assert_eq!(
            Coordinates::new(91.0, 15.0),
            Err(DomainError::InvalidLatitude(91.0))
        );
        assert!(Coordinates::new(-91.0, 15.0).is_err());
    }

    #[test]
    fn invalid_longitude() {
        assert_eq!(
            Coordinates::new(49.0, 181.0),
            Err(DomainError::InvalidLongitude(181.0))
        );
        assert!(Coordinates::new(49.0, -181.0).is_err());
    }
}
