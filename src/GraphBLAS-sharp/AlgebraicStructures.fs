namespace GraphBLAS.FSharp

open Microsoft.FSharp.Quotations

type UnaryOp<'a, 'b> =
    | UnaryOp of Expr<'a -> 'b>
    static member op_Implicit (UnaryOp source) = source

type BinaryOp<'a, 'b, 'c> =
    | BinaryOp of Expr<'a -> 'b -> 'c>
    static member op_Implicit (BinaryOp source) = source

// делать отдельными классами или оставить аллиасами
type ClosedUnaryOp<'a> =
    | ClosedUnaryOp of Expr<'a -> 'a>
    static member op_Implicit (UnaryOp source) = source

type ClosedBinaryOp<'a> =
    | ClosedBinaryOp of Expr<'a -> 'a -> 'a>
    static member op_Implicit (UnaryOp source) = source

// associative closed bin op (magma with associative)
type ISemigroup<'a> =
    abstract Plus: ClosedBinaryOp<'a>

// semigroup with id
type IMonoid<'a> =
    inherit ISemigroup<'a>
    abstract Zero: 'a

type ISemiring<'a> =
    inherit IMonoid<'a>
    abstract Times: ClosedBinaryOp<'a>
