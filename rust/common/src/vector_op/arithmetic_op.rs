use std::any::type_name;
use std::cmp::min;
use std::convert::TryInto;
use std::fmt::Debug;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedRem, CheckedSub};

use crate::error::ErrorCode::{InternalError, NumericValueOutOfRange};
use crate::error::{Result, RwError};
use crate::types::{IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper};

#[inline(always)]
pub fn general_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedAdd,
{
    general_atm(l, r, |a, b| match a.checked_add(&b) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    })
}

#[inline(always)]
pub fn general_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedSub,
{
    general_atm(l, r, |a, b| match a.checked_sub(&b) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    })
}

#[inline(always)]
pub fn general_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedMul,
{
    general_atm(l, r, |a, b| match a.checked_mul(&b) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    })
}

#[inline(always)]
pub fn general_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedDiv,
{
    general_atm(l, r, |a, b| match a.checked_div(&b) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    })
}

#[inline(always)]
pub fn general_mod<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedRem,
{
    general_atm(l, r, |a, b| match a.checked_rem(&b) {
        Some(c) => Ok(c),
        None => Err(RwError::from(NumericValueOutOfRange)),
    })
}

#[inline(always)]
pub fn general_atm<T1, T2, T3, F>(l: T1, r: T2, atm: F) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    F: FnOnce(T3, T3) -> Result<T3>,
{
    // TODO: We need to improve the error message
    let l: T3 = l.try_into().map_err(|_| {
        RwError::from(InternalError(format!(
            "Can't convert {} to {}",
            type_name::<T1>(),
            type_name::<T3>()
        )))
    })?;
    let r: T3 = r.try_into().map_err(|_| {
        RwError::from(InternalError(format!(
            "Can't convert {} to {}",
            type_name::<T2>(),
            type_name::<T3>()
        )))
    })?;
    atm(l, r)
}

const LEAP_DAYS: &[i32] = &[0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const NORMAL_DAYS: &[i32] = &[0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// return the days of the `year-month`
fn get_mouth_days(year: i32, month: usize) -> i32 {
    if is_leap_year(year) {
        LEAP_DAYS[month]
    } else {
        NORMAL_DAYS[month]
    }
}

#[inline(always)]
pub fn timestamp_timestamp_sub<T1, T2, T3>(
    l: NaiveDateTimeWrapper,
    r: NaiveDateTimeWrapper,
) -> Result<IntervalUnit> {
    let tmp = l.0 - r.0;
    Ok(IntervalUnit::new(0, tmp.num_days() as i32, 0))
}

#[inline(always)]
pub fn date_date_sub<T1, T2, T3>(l: NaiveDateWrapper, r: NaiveDateWrapper) -> Result<i32> {
    Ok((l.0 - r.0).num_days() as i32)
}

#[inline(always)]
pub fn interval_date_add<T1, T2, T3>(
    l: IntervalUnit,
    r: NaiveDateWrapper,
) -> Result<NaiveDateTimeWrapper> {
    let mut date = r.0;
    if l.get_months() != 0 {
        // NaiveDate don't support add months. We need calculate manually
        let mut day = date.day() as i32;
        let mut month = date.month() as i32;
        let mut year = date.year();
        // Calculate the number of year in this interval
        let interval_months = l.get_months();
        let year_diff = interval_months / 12;
        year += year_diff;

        // Calculate the number of month in this interval except the added year
        // The range of month_diff is (-12, 12) (The month is negative when the interval is
        // negative)
        let month_diff = interval_months - year_diff * 12;
        // The range of new month is (-12, 24) ( original month:[1, 12] + month_diff:(-12, 12) )
        month += month_diff;
        // Process the overflow months
        if month > 12 {
            year += 1;
            month -= 12;
        } else if month <= 0 {
            year -= 1;
            month += 12;
        }

        // Fix the days after changing date.
        // For example, 1970.1.31 + 1 month = 1970.2.28
        day = min(day, get_mouth_days(year, month as usize));
        date = NaiveDate::from_ymd(year, month as u32, day as u32);
    }
    let mut datetime = NaiveDateTime::new(date, NaiveTime::from_hms(0, 0, 0));
    datetime = datetime
        .checked_add_signed(Duration::days(l.get_days().into()))
        .ok_or_else(|| InternalError("Date out of range".to_string()))?;
    datetime = datetime
        .checked_add_signed(Duration::milliseconds(l.get_ms()))
        .ok_or_else(|| InternalError("Date out of range".to_string()))?;
    Ok(NaiveDateTimeWrapper::new(datetime))
}

#[inline(always)]
pub fn date_interval_add<T2, T1, T3>(
    l: NaiveDateWrapper,
    r: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    interval_date_add::<T1, T2, T3>(r, l)
}

#[inline(always)]
pub fn date_interval_sub<T2, T1, T3>(
    l: NaiveDateWrapper,
    r: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    interval_date_add::<T1, T2, T3>(r.negative(), l)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::types::Decimal;
    use crate::vector_op::arithmetic_op::general_add;

    #[test]
    fn test() {
        assert_eq!(
            general_add::<_, _, Decimal>(Decimal::from_str("1").unwrap(), 1i32).unwrap(),
            Decimal::from_str("2").unwrap()
        );
    }
}
