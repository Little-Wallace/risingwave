use std::marker::PhantomData;

use risingwave_pb::expr::expr_node::Type as ProstType;

/// For expression that only accept one value as input (e.g. CAST)
use super::template::{UnaryBytesExpression, UnaryExpression};
use crate::array::*;
use crate::expr::expr_is_null::{IsNotNullExpression, IsNullExpression};
use crate::expr::pg_sleep::PgSleepExpression;
use crate::expr::template::UnaryNullableExpression;
use crate::expr::BoxedExpression;
use crate::types::*;
use crate::vector_op::cast::*;
use crate::vector_op::cmp::{is_false, is_not_false, is_not_true, is_true};
use crate::vector_op::conjunction;
use crate::vector_op::length::length_default;
use crate::vector_op::ltrim::ltrim;
use crate::vector_op::rtrim::rtrim;
use crate::vector_op::trim::trim;
use crate::vector_op::upper::upper;

/// This macro helps to create cast expression.
/// It receives all the combinations of `gen_cast` and generates corresponding match cases
/// In `[]`, the parameters are for constructing new expression
/// * `$child`: child expression
/// * `$ret`: return expression
///
/// In `()*`, the parameters are for generating match cases
/// * `$input`: input type
/// * `$cast`: The cast type in that the operation will calculate
/// * `$func`: The scalar function for expression, it's a generic function and specialized by the
///   type of `$input, $cast`
macro_rules! gen_cast_impl {
  ([$child:expr, $ret:expr], $( { $input:tt, $cast:tt, $func:expr } ),*) => {
    match ($child.return_type(), $ret) {
      $(
          ($input! { type_match_pattern }, $cast! { type_match_pattern }) => {
            Box::new(UnaryExpression::< $input! { type_array }, $cast! { type_array }, _> {
              expr_ia1: $child,
              return_type: $ret,
              func: $func,
              _phantom: PhantomData,
            })
          }
      ),*
      _ => {
        unimplemented!("CAST({:?} AS {:?}) not supported yet!", $child.return_type(), $ret)
      }
    }
  };
}

macro_rules! gen_cast {
  ($($x:tt, )* ) => {
    gen_cast_impl! {
      [$($x),*],
      { varchar, date, str_to_date},
      { varchar, timestamp, str_to_timestamp},
      { varchar, timestampz, str_to_timestampz},
      { varchar, int16, str_to_i16},
      { varchar, int32, str_to_i32},
      { varchar, int64, str_to_i64},
      { varchar, float32, str_to_real},
      { varchar, float64, str_to_double},
      { varchar, decimal, str_to_decimal},
      { varchar, boolean, str_to_bool},
      { boolean, varchar, bool_to_str},
      // TODO: decide whether nullability-cast should be allowed (#2350)
      { boolean, boolean, |x| Ok(x)},
      { int16, int32, general_cast },
      { int16, int64, general_cast },
      { int16, float32, general_cast },
      { int16, float64, general_cast },
      { int16, decimal, general_cast },
      { int32, int16, general_cast },
      { int32, int64, general_cast },
      { int32, float64, general_cast },
      { int32, decimal, general_cast },
      { int64, int16, general_cast },
      { int64, int32, general_cast },
      { int64, decimal, general_cast },
      { float32, float64, general_cast },
      { float32, decimal, general_cast },
      { float32, int16, to_i16 },
      { float32, int32, to_i32 },
      { float32, int64, to_i64 },
      { float64, decimal, general_cast },
      { float64, int16, to_i16 },
      { float64, int32, to_i32 },
      { float64, int64, to_i64 },
      { decimal, decimal, dec_to_dec },
      { decimal, int16, deci_to_i16 },
      { decimal, int32, deci_to_i32 },
      { decimal, int64, deci_to_i64 },
      { date, timestamp, date_to_timestamp }
    }
  };
}

pub fn new_unary_expr(
    expr_type: ProstType,
    return_type: DataTypeKind,
    child_expr: BoxedExpression,
) -> BoxedExpression {
    use crate::expr::data_types::*;

    match (expr_type, return_type, child_expr.return_type()) {
        // FIXME: We can not unify char and varchar because they are different in PG while share the
        // same logical type (String type) in our system (#2414).
        (ProstType::Cast, DataTypeKind::Date, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, NaiveDateArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_date,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Time, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, NaiveTimeArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_time,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Timestamp, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, NaiveDateTimeArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_timestamp,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Timestampz, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_timestampz,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Boolean, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, BoolArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_bool,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Decimal { .. }, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, DecimalArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_decimal,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Float32, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, F32Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_real,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Float64, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, F64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_double,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Int16, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I16Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_i16,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Int32, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I32Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_i32,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Int64, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, I64Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_i64,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Char, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, Utf8Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_str,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, DataTypeKind::Varchar, DataTypeKind::Char) => {
            Box::new(UnaryExpression::<Utf8Array, Utf8Array, _> {
                expr_ia1: child_expr,
                return_type,
                func: str_to_str,
                _phantom: PhantomData,
            })
        }
        (ProstType::Cast, _, _) => gen_cast! {child_expr, return_type, },
        (ProstType::Not, _, _) => Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _> {
            expr_ia1: child_expr,
            return_type,
            func: conjunction::not,
            _phantom: PhantomData,
        }),
        (ProstType::IsTrue, _, _) => Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _> {
            expr_ia1: child_expr,
            return_type,
            func: is_true,
            _phantom: PhantomData,
        }),
        (ProstType::IsNotTrue, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: is_not_true,
                _phantom: PhantomData,
            })
        }
        (ProstType::IsFalse, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: is_false,
                _phantom: PhantomData,
            })
        }
        (ProstType::IsNotFalse, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _> {
                expr_ia1: child_expr,
                return_type,
                func: is_not_false,
                _phantom: PhantomData,
            })
        }
        (ProstType::IsNull, _, _) => Box::new(IsNullExpression::new(child_expr)),
        (ProstType::IsNotNull, _, _) => Box::new(IsNotNullExpression::new(child_expr)),
        (ProstType::Upper, _, _) => Box::new(UnaryBytesExpression::<Utf8Array, _> {
            expr_ia1: child_expr,
            return_type,
            func: upper,
            _phantom: PhantomData,
        }),
        (ProstType::PgSleep, _, DataTypeKind::Decimal { .. }) => {
            Box::new(PgSleepExpression::new(child_expr))
        }
        (expr, ret, child) => {
            unimplemented!("The expression {:?}({:?}) ->{:?} using vectorized expression framework is not supported yet!", expr, child, ret)
        }
    }
}

pub fn new_length_default(expr_ia1: BoxedExpression, return_type: DataTypeKind) -> BoxedExpression {
    Box::new(UnaryExpression::<Utf8Array, I64Array, _> {
        expr_ia1,
        return_type,
        func: length_default,
        _phantom: PhantomData,
    })
}

pub fn new_trim_expr(expr_ia1: BoxedExpression, return_type: DataTypeKind) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _> {
        expr_ia1,
        return_type,
        func: trim,
        _phantom: PhantomData,
    })
}

pub fn new_ltrim_expr(expr_ia1: BoxedExpression, return_type: DataTypeKind) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _> {
        expr_ia1,
        return_type,
        func: ltrim,
        _phantom: PhantomData,
    })
}

pub fn new_rtrim_expr(expr_ia1: BoxedExpression, return_type: DataTypeKind) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _> {
        expr_ia1,
        return_type,
        func: rtrim,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use itertools::Itertools;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::FunctionCall;

    use super::super::*;
    use crate::array::column::Column;
    use crate::array::*;
    use crate::expr::test_utils::{make_expression, make_input_ref};
    use crate::types::{NaiveDateWrapper, Scalar};
    use crate::vector_op::cast::{date_to_timestamp, str_to_i16};

    #[test]
    fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, Type::Not);
        test_unary_date::<NaiveDateTimeArray, _>(|x| date_to_timestamp(x).unwrap(), Type::Cast);
        test_str_to_int16::<I16Array, _>(|x| str_to_i16(x).unwrap());
    }

    #[test]
    fn test_i16_to_i32() {
        let mut input = Vec::<Option<i16>>::new();
        let mut target = Vec::<Option<i32>>::new();
        for i in 0..100i16 {
            if i % 2 == 0 {
                target.push(Some(i as i32));
                input.push(Some(i as i16));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = Column::new(
            I16Array::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let return_type = DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Int16)],
            })),
        };
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_str_to_int16<A, F>(f: F)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(&str) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<String>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..1u32 {
            if i % 2 == 0 {
                let s = i.to_string();
                target.push(Some(f(&s)));
                input.push(Some(s));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = Column::new(
            Utf8Array::from_slice(&input.iter().map(|x| x.as_ref().map(|x| &**x)).collect_vec())
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let return_type = DataType {
            type_name: TypeName::Int16 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Char)],
            })),
        };
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_unary_bool<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(bool) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<bool>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                input.push(Some(true));
                target.push(Some(f(true)));
            } else if i % 3 == 0 {
                input.push(Some(false));
                target.push(Some(f(false)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = Column::new(
            BoolArray::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let expr = make_expression(kind, &[TypeName::Boolean], &[0]);
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }

    fn test_unary_date<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(NaiveDateWrapper) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<NaiveDateWrapper>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                let date = NaiveDateWrapper::new(NaiveDate::from_num_days_from_ce(i));
                input.push(Some(date));
                target.push(Some(f(date)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = Column::new(
            NaiveDateArray::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::builder().columns(vec![col1]).build();
        let expr = make_expression(kind, &[TypeName::Date], &[0]);
        let mut vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }
    }
}
