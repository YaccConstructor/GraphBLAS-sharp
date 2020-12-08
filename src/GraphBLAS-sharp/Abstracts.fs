namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>(nrow: int, ncol: int) =
    abstract RowCount: int
    abstract ColumnCount: int
    default this.RowCount = nrow
    default this.ColumnCount = ncol

    abstract Mask: Mask2D option
    abstract Complemented: Mask2D option

    abstract Item: Mask2D option -> Matrix<'a> with get, set
    abstract Item: Mask1D option * int -> Vector<'a> with get, set
    abstract Item: int * Mask1D option -> Vector<'a> with get, set
    abstract Item: int * int -> Scalar<'a> with get, set
    abstract Fill: Mask2D option -> Scalar<'a> with set
    abstract Fill: Mask1D option * int -> Scalar<'a> with set
    abstract Fill: int * Mask1D option -> Scalar<'a> with set

    abstract Mxm: Matrix<'a> -> Mask2D option -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Matrix<'a> -> Mask2D option -> Monoid<'a> -> Matrix<'a>
    abstract EWiseMult: Matrix<'a> -> Mask2D option -> Monoid<'a> -> Matrix<'a>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract ReduceIn: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract ReduceOut: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract Reduce: Monoid<'a> -> Scalar<'a>
    abstract T: Matrix<'a>

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (@.) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (@.) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y

and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>(length: int) =
    abstract Length: int
    default this.Length = length

    abstract AsArray: 'a[]
    abstract Clear: unit -> unit

    abstract Mask: Mask1D option
    abstract Complemented: Mask1D option

    abstract Item: Mask1D option -> Vector<'a> with get, set
    abstract Item: int -> Scalar<'a> with get, set
    abstract Fill: Mask1D option -> Scalar<'a> with set

    abstract Vxm: Matrix<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Vector<'a> -> Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Reduce: Monoid<'a> -> Scalar<'a>

    static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    static member inline (@.) (x: Vector<'a>, y: Matrix<'a>) = x.Vxm y

and Mask1D(indices: int[], length: int, isComplemented: bool) =
    member this.Indices = indices
    member this.Length = length
    member this.IsComplemented = isComplemented

    member this.Item
        with get (idx: int) : bool =
            this.Indices
            |> Array.exists ((=) idx)
            |> (<>) this.IsComplemented

and Mask2D(indices: (int * int)[], rowCount: int, columnCount: int, isComplemented: bool) =
    member this.Rows = indices |> Array.unzip |> fst
    member this.Columns = indices |> Array.unzip |> snd
    member this.RowCount = rowCount
    member this.ColumnCount = columnCount
    member this.IsComplemented = isComplemented

    member this.Item
        with get (rowIdx: int, colIdx: int) : bool =
            (this.Rows, this.Columns)
            ||> Array.zip
            |> Array.exists ((=) (rowIdx, colIdx))
            |> (<>) this.IsComplemented
