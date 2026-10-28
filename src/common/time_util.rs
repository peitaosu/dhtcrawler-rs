use chrono::{DateTime, Local, TimeZone};

pub fn now_seconds() -> i64 {
    Local::now().timestamp()
}

pub fn day_seconds(timestamp: i64) -> i64 {
    let local: DateTime<Local> = Local.timestamp_opt(timestamp, 0).single().unwrap_or_default();
    local.date_naive().and_hms_opt(0, 0, 0).map_or(timestamp, |midnight| {
        Local
            .from_local_datetime(&midnight)
            .single()
            .map(|dt| dt.timestamp())
            .unwrap_or(timestamp)
    })
}

pub fn now_day_seconds() -> i64 {
    day_seconds(now_seconds())
}

pub fn format_datetime(seconds: i64) -> String {
    let local: DateTime<Local> = Local.timestamp_opt(seconds, 0).single().unwrap_or_default();
    local.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn format_date(seconds: i64) -> String {
    let local: DateTime<Local> = Local.timestamp_opt(seconds, 0).single().unwrap_or_default();
    local.format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_seconds_is_midnight() {
        use chrono::Timelike;
        let now = now_seconds();
        let day = day_seconds(now);
        assert!(day <= now);
        assert!(now - day < 24 * 3600);
        let local: DateTime<Local> = Local.timestamp_opt(day, 0).single().unwrap();
        assert_eq!(local.time().num_seconds_from_midnight(), 0);
    }

    #[test]
    fn format_works() {
        assert!(format_datetime(0).len() == 19);
        assert!(format_date(0).len() == 10);
    }
}
