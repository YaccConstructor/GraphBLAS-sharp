namespace GraphBLAS.FSharp

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>() =
    abstract RowCount: int
    abstract ColumnCount: int

    abstract Mask: Option<Mask2D>
    abstract Complemented: Option<Mask2D>

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

    abstract Mask: Option<Mask1D>
    abstract Complemented: Option<Mask1D>

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

and Mask1D(indices: int[], length: int, isComplemented: bool) =
    member this.IsComplemented = isComplemented

    member this.Indices = indices
    member this.Length = length

    member this.Item
        with get (idx: int) : bool =
            this.Indices |> Array.exists ( ( = ) idx) |> ( <> ) this.IsComplemented

and Mask2D(indices: (int * int)[], rowCount: int, columnCount: int, isComplemented: bool) =
    member this.IsComplemented = isComplemented

    member this.Rows = indices |> Array.unzip |> fst
    member this.Columns = indices |> Array.unzip |> snd
    member this.RowCount = rowCount
    member this.ColumnCount = columnCount

    member this.Item
        with get (rowIdx: int, colIdx: int) : bool =
            Array.zip this.Rows this.Columns |> Array.exists ( ( = ) (rowIdx, colIdx)) |> ( <> ) this.IsComplemented
