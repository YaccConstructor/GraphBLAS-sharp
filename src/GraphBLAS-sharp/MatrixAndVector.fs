namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>(nrow: int, ncol: int) =
    abstract RowCount: int
    abstract ColumnCount: int
    default this.RowCount = nrow
    default this.ColumnCount = ncol

    abstract Extract: Mask2D<'t> -> Matrix<'a>
    abstract Extract: (Mask1D<'t> * int) -> Vector<'a>
    abstract Extract: (int * Mask1D<'t>) -> Vector<'a>
    abstract Extract: (int * int) -> Scalar<'a>

    abstract Assign: Mask2D<'t> * Matrix<'a> -> unit
    abstract Assign: (Mask1D<'t> * int) * Vector<'a> -> unit
    abstract Assign: (int * Mask1D<'t>) * Vector<'a> -> unit
    abstract Assign: (int * int) * Scalar<'a> -> unit
    abstract Assign: Mask2D<'t> * Scalar<'a> -> unit
    abstract Assign: (Mask1D<'t> * int) * Scalar<'a> -> unit
    abstract Assign: (int * Mask1D<'t>) * Scalar<'a> -> unit

    abstract Mxm: Matrix<'a> -> Mask2D<'t> -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Mask1D<'t> -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Matrix<'a> -> Mask2D<'t> -> Semiring<'a> -> Matrix<'a>
    abstract EWiseMult: Matrix<'a> -> Mask2D<'t> -> Semiring<'a> -> Matrix<'a>
    abstract Apply: Mask1D<'t> -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract ReduceIn: Mask1D<'t> -> Monoid<'a> -> Vector<'a>
    abstract ReduceOut: Mask1D<'t> -> Monoid<'a> -> Vector<'a>
    abstract T: Matrix<'a>

    abstract EWiseAddInplace: Matrix<'a> -> Mask2D<'t> -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Matrix<'a> -> Mask2D<'t> -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask2D<'t> -> UnaryOp<'a, 'b> -> unit

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (+.*) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y
    static member inline (.+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAddInplace y
    static member inline (.*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMultInplace y

and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>(size: int) =
    abstract Length: int
    default this.Length = size

    abstract AsArray: 'a[]

    abstract Extract: Mask1D<'t> -> Vector<'a>
    abstract Extract: int -> Scalar<'a>

    abstract Assign: Vector<'a> * Mask1D<'t>-> unit
    abstract Assign: Scalar<'a> * int -> unit
    abstract Assign: Scalar<'a> * Mask1D<'t> -> unit

    abstract Vxm: Matrix<'b> -> Mask1D<'t> -> Semiring<'a, 'b, 'c> -> Vector<'c>
    abstract EWiseAdd: Vector<'a> -> Mask1D<'t> -> Semiring<'a> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D<'t> -> Semiring<'a> -> Vector<'a>
    abstract Apply: Mask1D<'t> -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Reduce: Monoid<'a> -> Scalar<'a>

    abstract VxmInplace: Matrix<'a> -> Mask1D<'t> -> Semiring<'a> -> unit
    abstract EWiseAddInplace: Vector<'a> -> Mask1D<'t> -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Vector<'a> -> Mask1D<'t> -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask1D<'t> -> UnaryOp<'a, 'b> -> unit

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
