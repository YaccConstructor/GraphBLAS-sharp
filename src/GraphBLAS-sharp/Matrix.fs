namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a>() =
    abstract RowCount: int
    abstract ColumnCount: int

    abstract Mxm: Matrix<'a> -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Semiring<'a> -> Vector<'a>
    abstract EWiseMult: Matrix<'a> -> Semiring<'a> -> Matrix<'a>
    abstract EWiseAdd: Matrix<'a> -> Semiring<'a> -> Matrix<'a>
    abstract Apply: UnaryOp<'a, 'b> -> Semiring<'a> -> Matrix<'b>
    abstract ReduceIn: Monoid<'a> -> Vector<'b>
    abstract ReduceOut: Monoid<'a> -> Vector<'b>
    abstract T: Matrix<'a>

    abstract EWiseMultInplace: Matrix<'a> -> Semiring<'a> -> unit
    abstract EWiseAddInplace: Matrix<'a> -> Semiring<'a> -> unit
    abstract ApplyInplace: UnaryOp<'a, 'b> -> Semiring<'a> -> unit

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (+.*) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y
    static member inline (.+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMultInplace y
    static member inline (.*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAddInplace y

and [<AbstractClass>] Vector<'a>() =
    abstract Length: int

    abstract Mxm: Matrix<'a> -> Semiring<'a> -> Matrix<'a>
