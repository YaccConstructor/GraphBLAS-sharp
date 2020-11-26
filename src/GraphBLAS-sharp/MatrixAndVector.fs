namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>() =
    abstract RowCount: int
    abstract ColumnCount: int

    abstract CreateMask: bool -> Mask2D

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

    abstract CreateMask: bool -> Mask1D

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

and Mask1D(isNone: bool, indices: int[], length: int, isRegular: bool) =
    member this.IsNone = isNone
    member this.IsRegular = isRegular

    member this.Indices = indices
    member this.Length = length

    member this.Item
        with get (idx: int) : bool =
            this.IsNone || this.Indices |> Array.exists ( ( = ) idx) |> ( = ) this.IsRegular

    static member Create (vector: Vector<'a>) = vector.CreateMask true
    static member Complemented (vector: Vector<'a>) = vector.CreateMask false
    static member None = Mask1D(true, Array.empty, 0, true)

and Mask2D(isNone: bool, rows: int[], columns: int[], rowCount: int, columnCount: int, isRegular: bool) =
    member this.IsNone = isNone
    member this.IsRegular = isRegular

    member this.Rows = rows
    member this.Columns = columns
    member this.RowCount = rowCount
    member this.ColumnCount = columnCount

    member this.Item
        with get (rowIdx: int, colIdx: int) : bool =
            this.IsNone || Array.zip this.Rows this.Columns |> Array.exists ( ( = ) (rowIdx, colIdx)) |> ( = ) this.IsRegular

    static member Create (matrix: Matrix<'a>) = matrix.CreateMask true
    static member Complemented (matrix: Matrix<'a>) = matrix.CreateMask false
    static member None = Mask2D(true, Array.empty, Array.empty, 0, 0, true)
