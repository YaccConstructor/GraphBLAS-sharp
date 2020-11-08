namespace GraphBLAS.FSharp

type Semiring<'a> = {
    PlusMonoid: Monoid<'a>
    Times: BinaryOp<'a, 'a, 'a>
}

[<Struct>]
type MinPlusSemiring = MinPlusSemiring of float
with
    static member Zero = MinPlusSemiring System.Double.PositiveInfinity
    static member (+) (MinPlusSemiring x, MinPlusSemiring y) = System.Math.Min(x, y) |> MinPlusSemiring
    static member (*) (MinPlusSemiring x, MinPlusSemiring y) = x + y |> MinPlusSemiring
    static member op_Implicit (MinPlusSemiring source) = source

module Semiring =
    let create<'a> (zero: 'a) (plus: BinaryOp<'a, 'a, 'a>) (mult: BinaryOp<'a, 'a, 'a>) =
        {
            PlusMonoid = {
                Zero = zero
                Append = plus
            }
            Times = mult
        }

    create<MinPlusSemiring>
        LanguagePrimitives.GenericZero
        (BinaryOp <@ ( + ) @>)
        (BinaryOp <@ ( * ) @>)
    |> ignore


// 1 вар - создавать семиринги, передавая в create все данные
// 2 вар - сделать фабрику таких контекстов из сущ классов

// 3 вар - глобальное состояние для вычислительного контекста


// [<Struct>]
// type MinPlusSemiring = MinPlusSemiring of float
// with
//     static member Zero = MinPlusSemiring System.Double.PositiveInfinity
//     static member (+) (MinPlusSemiring x, MinPlusSemiring y) = System.Math.Min(x, y) |> MinPlusSemiring
//     static member (*) (MinPlusSemiring x, MinPlusSemiring y) = x + y |> MinPlusSemiring
//     static member op_Implicit (MinPlusSemiring source) : float = source

// [<Struct>]
// type StandardSemiring = StandardSemiring of float
// with
//     static member Zero = StandardSemiring System.Double.PositiveInfinity
//     static member (+) (StandardSemiring x, StandardSemiring y) = x + y |> StandardSemiring
//     static member (*) (StandardSemiring x, StandardSemiring y) = x * y |> StandardSemiring
//     static member op_Implicit (StandardSemiring source) : float = source
