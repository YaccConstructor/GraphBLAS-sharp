namespace GraphBLAS.FSharp

open Microsoft.FSharp.Quotations

type UnaryOp<'a, 'b> = UnaryOp of Expr<'a -> 'b>
type BinaryOp<'a, 'b, 'c> = BinaryOp of Expr<'a -> 'b -> 'c>

type ClosedUnaryOp<'a> = ClosedUnaryOp of Expr<'a -> 'a>
type ClosedBinaryOp<'a> = ClosedBinaryOp of Expr<'a -> 'a -> 'a>

// associative closed bin op (magma with associative)
type ISemigroup<'a> =
    abstract Plus: ClosedBinaryOp<'a>

/// Semigroup with identity
type IMonoid<'a> =
    inherit ISemigroup<'a>
    abstract Zero: 'a

type ISemiring<'a> =
    inherit IMonoid<'a>
    abstract Times: ClosedBinaryOp<'a>

(*
    мотивация:
    хотим, чтобы ноль был нулем (даже если он явно в матрице хранится)
    и все моноиды, определенные над MonoidicType 'a имели корректную семантику
    (если получился 0 и мы сменили моноид, то этот элемент все еще будет нулем в другом моноиде)
*)

[<Struct>]
type MonoidicType<'a> =
    | Just of 'a
    | Zero

module MonoidicType =
    let wrap (isZero: 'a -> bool) x =
        if isZero x then Zero
        else Just x
