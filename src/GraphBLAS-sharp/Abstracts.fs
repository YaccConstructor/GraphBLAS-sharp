namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>(nrow: int, ncol: int) =
    abstract RowCount: int
    abstract ColumnCount: int
    default this.RowCount = nrow
    default this.ColumnCount = ncol

    abstract Item: Mask2D -> Matrix<'a> with get, set
    abstract Item: Mask1D * int -> Vector<'a> with get, set
    abstract Item: int * Mask1D -> Vector<'a> with get, set
    abstract Item: int * int -> Scalar<'a> with get, set
    abstract Fill: Mask2D -> Scalar<'a> with set
    abstract Fill: Mask1D * int -> Scalar<'a> with set
    abstract Fill: int * Mask1D -> Scalar<'a> with set

    abstract Mxm: Matrix<'b> -> Mask2D -> Semiring<'a, 'b, 'c> -> Matrix<'c>
    abstract Mxv: Vector<'b> -> Mask1D -> Semiring<'a, 'b, 'c> -> Vector<'c>
    abstract EWiseAdd: Matrix<'a> -> Mask2D -> BinaryOp<'a, 'b, 'c> -> Matrix<'a>
    abstract EWiseMult: Matrix<'a> -> Mask2D -> BinaryOp<'a, 'b, 'c> -> Matrix<'a>
    abstract Apply: Mask1D -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract ReduceIn: Mask1D -> BinaryOp<'a, 'b, 'c> -> Vector<'a>
    abstract ReduceOut: Mask1D -> BinaryOp<'a, 'b, 'c> -> Vector<'a>
    abstract Reduce: Monoid<'a> -> Scalar<'a>
    abstract T: Matrix<'a>

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (.@) (x: Matrix<'a>, y: Matrix<'b>) = x.Mxm y
    static member inline (.@) (x: Matrix<'a>, y: Vector<'b>) = x.Mxv y

and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>(size: int) =
    abstract Size: int
    default this.Size = size

    abstract AsArray: 'a[]
    abstract Clear: unit -> unit

    abstract Item: Mask1D -> Vector<'a> with get, set
    abstract Item: int -> Scalar<'a> with get, set
    abstract Fill: Mask1D -> Scalar<'a> with set

    abstract Vxm: Matrix<'b> -> Mask1D -> Semiring<'a, 'b, 'c> -> Vector<'c>
    abstract EWiseAdd: Vector<'a> -> Mask1D -> BinaryOp<'a, 'b, 'c> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D -> BinaryOp<'a, 'b, 'c> -> Vector<'a>
    abstract Apply: Mask1D -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Reduce: Monoid<'a> -> Scalar<'a>

    static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    static member inline (.@) (x: Vector<'a>, y: Matrix<'b>) = x.Vxm y

and Mask1D = {
    Indices: int list
    Size: int
    IsComplemented: bool
}

and Mask2D = {
    Indices: (int * int) list
    RowCount: int
    ColumnCount: int
    IsComplemented: bool
}
