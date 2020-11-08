namespace GraphBLAS.FSharp

open Microsoft.FSharp.Quotations

[<Struct>]
type UnaryOp<'a, 'b> = UnaryOp of Expr<'a -> 'b>
with
    static member op_Implicit (UnaryOp source) = source


[<Struct>]
type BinaryOp<'a, 'b, 'c> = BinaryOp of Expr<'a -> 'b -> 'c>
with
    static member op_Implicit (BinaryOp source) = source
