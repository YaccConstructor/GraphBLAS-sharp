namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>() =
    abstract RowCount: int
    abstract ColumnCount: int

    abstract Item: Mask2D<'a> -> Matrix<'a> with get, set
    abstract Item: Mask1D<'a> * int -> Vector<'a> with get, set
    abstract Item: int * Mask1D<'a> -> Vector<'a> with get, set
    abstract Item: int * int -> Scalar<'a> with get, set
    abstract Item: Mask2D<'a> -> Scalar<'a> with set
    abstract Item: Mask1D<'a> * int -> Scalar<'a> with set
    abstract Item: int * Mask1D<'a> -> Scalar<'a> with set

    abstract Mxm: Matrix<'a> -> Mask2D<'a> -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Mask1D<'a> -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Matrix<'a> -> Mask2D<'a> -> Semiring<'a> -> Matrix<'a>
    abstract EWiseMult: Matrix<'a> -> Mask2D<'a> -> Semiring<'a> -> Matrix<'a>
    abstract Apply: Mask1D<'a> -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract ReduceIn: Mask1D<'a> -> Monoid<'a> -> Vector<'a>
    abstract ReduceOut: Mask1D<'a> -> Monoid<'a> -> Vector<'a>
    abstract T: Matrix<'a>

    abstract EWiseAddInplace: Matrix<'a> -> Mask2D<'a> -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Matrix<'a> -> Mask2D<'a> -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask2D<'a> -> UnaryOp<'a, 'b> -> unit

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (+.*) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y
    static member inline (.+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAddInplace y
    static member inline (.*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMultInplace y

and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>() =
    abstract Length: int
    abstract AsArray: 'a[]

    // abstract Item: Mask1D<'b> -> Vector<'a> with get, set
    // abstract Item: int -> Scalar<'a> with get, set
    // abstract Item: Mask1D<'c> -> Scalar<'a> with set

    abstract Assign: Mask1D<'b> * Vector<'a> -> unit
    abstract Assign: int * Scalar<'a> -> unit
    abstract Assign: Mask1D<'b> * Scalar<'a> -> unit


    abstract Vxm: Matrix<'a> -> Mask1D<'b> -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Vector<'a> -> Mask1D<'b> -> Semiring<'a> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D<'b> -> Semiring<'a> -> Vector<'a>
    abstract Apply: Mask1D<'c> -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Reduce: Monoid<'a> -> Scalar<'a>

    abstract EWiseAddInplace: Vector<'a> -> Mask1D<'b> -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Vector<'a> -> Mask1D<'b> -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask1D<'c> -> UnaryOp<'a, 'b> -> unit

    static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Vector<'a>, y: Matrix<'a>) = x.Vxm y
    static member inline (.+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAddInplace y
    static member inline (.*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMultInplace y

and Mask1D<'a when 'a : struct and 'a : equality> =
    | Mask1D of Vector<'a>
    | Complemented1D of Vector<'a>
    | None

and Mask2D<'a when 'a : struct and 'a : equality> =
    | Mask2D of Matrix<'a>
    | Complemented2D of Matrix<'a>
    | None
