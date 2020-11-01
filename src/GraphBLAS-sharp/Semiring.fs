namespace GraphBLAS.FSharp

open Microsoft.FSharp.Quotations

type Semiring<'a> = {
    Addition: Expr<'a -> 'a -> 'a>
    Multiplication: Expr<'a -> 'a -> 'a>
}

module Semiring =
    let inline create< ^a
                when ^a: (static member ( + ) : ^a * ^a -> ^a)
                and ^a: (static member ( * ) : ^a * ^a -> ^a) >
        (plus: Expr< ^a -> ^a -> ^a >)
        (mult: Expr< ^a -> ^a -> ^a>) =
        { Addition = plus; Multiplication = mult }
