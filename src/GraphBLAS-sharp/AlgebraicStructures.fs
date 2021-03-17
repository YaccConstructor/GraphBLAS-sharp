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

// на самом деле должен быть "зависымым типом"
// (если получает ноль, то Zero, иначе Just x)
// но он будет оборачивать 'a только на стороне F#
// => можно сделать специальный статик коснтруктор для инстансов
// (если так делать, то уже не получится оставить его генерик типом)
// (можно в конструктор передавать проверку на ноль первым параметром, тогда норм)
[<Struct>]
type MonoidicType<'a> =
    | Just of 'a
    | Zero

(*
    мотивация
    хотим, чтобы ноль был нулем (даже если он в матрице будет хранится)
    и все моноиды, определенные над MonoidicType 'a имели корректную семантику
    (если получился 0 и мы сменили моноид, то этот элемент все еще будет нудем в другом моноиде)
    то избавимся от кастов
*)
