namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>() =
    abstract RowCount: int
    abstract ColumnCount: int

    abstract Item: Mask2D -> Matrix<'a> with get, set
    abstract Item: Mask1D * int -> Vector<'a> with get, set
    abstract Item: int * Mask1D -> Vector<'a> with get, set
    abstract Item: int * int -> Scalar<'a> with get, set
    abstract Item: Mask2D -> Scalar<'a> with set
    abstract Item: Mask1D * int -> Scalar<'a> with set
    abstract Item: int * Mask1D -> Scalar<'a> with set

    abstract Mxm: Matrix<'a> -> Mask2D -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Mask1D -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Matrix<'a> -> Mask2D -> Semiring<'a> -> Matrix<'a>
    abstract EWiseMult: Matrix<'a> -> Mask2D -> Semiring<'a> -> Matrix<'a>
    abstract Apply: Mask1D -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract ReduceIn: Mask1D -> Monoid<'a> -> Vector<'a>
    abstract ReduceOut: Mask1D -> Monoid<'a> -> Vector<'a>
    abstract T: Matrix<'a>

    abstract EWiseAddInplace: Matrix<'a> -> Mask2D -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Matrix<'a> -> Mask2D -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask2D -> UnaryOp<'a, 'b> -> unit

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (+.*) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y
    static member inline (.+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAddInplace y
    static member inline (.*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMultInplace y

and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>() =
    abstract Length: int
    abstract AsArray: 'a[]

    abstract Item: Mask1D -> Vector<'a> with get, set
    abstract Item: int -> Scalar<'a> with get, set
    abstract Item: Mask1D -> Scalar<'a> with set

    abstract Vxm: Matrix<'a> -> Mask1D -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Vector<'a> -> Mask1D -> Semiring<'a> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D -> Semiring<'a> -> Vector<'a>
    abstract Apply: Mask1D -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Reduce: Monoid<'a> -> Scalar<'a>

    abstract EWiseAddInplace: Vector<'a> -> Mask1D -> Semiring<'a> -> unit
    abstract EWiseMultInplace: Vector<'a> -> Mask1D -> Semiring<'a> -> unit
    abstract ApplyInplace: Mask1D -> UnaryOp<'a, 'b> -> unit

    static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    static member inline (+.*) (x: Vector<'a>, y: Matrix<'a>) = x.Vxm y
    static member inline (.+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAddInplace y
    static member inline (.*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMultInplace y

// and Mask1D<'a when 'a : struct and 'a : equality> =
//     | Mask1D of Vector<'a>
//     | Complemented1D of Vector<'a>
//     | None

// and Mask2D<'a when 'a : struct and 'a : equality> =
//     | Mask2D of Matrix<'a>
//     | Complemented2D of Matrix<'a>
//     | None

and [<AbstractClass>] Mask1D() =
    abstract Length: Option<int>

    abstract Item: int -> bool with get, set

and [<AbstractClass>] Mask2D() =
    abstract Size: Option<int * int>

    member this.RowCount: Option<int> =
        this.Size |> Option.bind (fst >> Some)
    member this.ColumnCount: Option<int> =
        this.Size |> Option.bind (snd >> Some)

    abstract Item: int * int -> bool with get, set
