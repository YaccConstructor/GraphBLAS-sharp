namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a>() =
    abstract RowCount: int
    abstract ColumnCount: int

    // abstract Mxm: Matrix<'a> -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Semiring<'a> -> Vector<'a>
    // abstract EWiseAdd: Matrix<'a> -> Semiring<'a> -> Matrix<'a>
    // abstract EWiseMult: Matrix<'a> -> Semiring<'a> -> Matrix<'a>
    // abstract Apply: UnaryOp<'a, 'b> -> Matrix<'b>
    // abstract ReduceIn: Monoid<'a> -> Vector<'a>
    // abstract ReduceOut: Monoid<'a> -> Vector<'a>
    // abstract T: Matrix<'a>

    // abstract EWiseAddInplace: Matrix<'a> -> Semiring<'a> -> unit
    // abstract EWiseMultInplace: Matrix<'a> -> Semiring<'a> -> unit
    // abstract ApplyInplace: UnaryOp<'a, 'b> -> unit

    // static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    // static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    // static member inline (+.*) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (+.*) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y
    // static member inline (.+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAddInplace y
    // static member inline (.*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMultInplace y

and [<AbstractClass>] Vector<'a>() =
    abstract Length: int
    abstract AsArray: 'a[]

    // abstract Vxm: Matrix<'a> -> Semiring<'a> -> Vector<'a>
    // abstract EWiseAdd: Vector<'a> -> Semiring<'a> -> Vector<'a>
    // abstract EWiseMult: Vector<'a> -> Semiring<'a> -> Vector<'a>
    // abstract Apply: UnaryOp<'a, 'b> -> Vector<'b>
    // abstract Reduce: Monoid<'a> -> Scalar<'a>

    // abstract EWiseAddInplace: Vector<'a> -> Semiring<'a> -> unit
    // abstract EWiseMultInplace: Vector<'a> -> Semiring<'a> -> unit
    // abstract ApplyInplace: UnaryOp<'a, 'b> -> unit

    // static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    // static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    // static member inline (+.*) (x: Vector<'a>, y: Matrix<'a>) = x.Vxm y
    // static member inline (.+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAddInplace y
    // static member inline (.*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMultInplace y
