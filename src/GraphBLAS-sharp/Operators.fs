namespace GraphBLAS.FSharp

open Microsoft.FSharp.Quotations

type UnaryOp<'a, 'b> = UnaryOp of Expr<'a -> 'b>
with
    static member op_Implicit (UnaryOp source) = source


type BinaryOp<'a, 'b, 'c> = BinaryOp of Expr<'a -> 'b -> 'c>
with
    static member op_Implicit (BinaryOp source) = source
