namespace GraphBLAS.FSharp

open Microsoft.FSharp.Quotations

type UnaryOp<'TIn, 'TOut> = UnaryOp of Expr<'TIn -> 'TOut>
with
    static member op_Implicit (UnaryOp source) = source


type BinaryOp<'T1, 'T2, 'TOut> = BinaryOp of Expr<'T1 -> 'T2 -> 'TOut>
with
    static member op_Implicit (BinaryOp source) = source
