namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>(nrow: int, ncol: int) =
    abstract RowCount: int
    abstract ColumnCount: int
    default this.RowCount = nrow
    default this.ColumnCount = ncol

    abstract Extract: Mask2D option -> Matrix<'a>
    abstract Extract: (Mask1D option * int) -> Vector<'a>
    abstract Extract: (int * Mask1D option) -> Vector<'a>
    abstract Extract: (int * int) -> Scalar<'a>
    // Размерности должны совпадать
    abstract Assign: Mask2D option * Matrix<'a> -> unit
    abstract Assign: (Mask1D option * int) * Vector<'a> -> unit
    abstract Assign: (int * Mask1D option) * Vector<'a> -> unit
    abstract Assign: (int * int) * Scalar<'a> -> unit
    abstract Assign: Mask2D option * Scalar<'a> -> unit
    abstract Assign: (Mask1D option * int) * Scalar<'a> -> unit
    abstract Assign: (int * Mask1D option) * Scalar<'a> -> unit
    // abstract Resize
    // abstract Dup
    // abstract Clear
    // abstract NNZ
    // abstract Tuples: OpenCLEvaluation<{| Rows: int[]; Columns: int[]; Values: 'a[] |}>

    abstract Mxm: Matrix<'a> -> Mask2D option -> Semiring<'a> -> Matrix<'a>
    abstract Mxv: Vector<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Matrix<'a> -> Mask2D option -> Monoid<'a> -> Matrix<'a>
    abstract EWiseMult: Matrix<'a> -> Mask2D option -> Monoid<'a> -> Matrix<'a>
    abstract Apply: Mask2D option -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract Prune: Mask2D option -> UnaryOp<'a, bool> -> Matrix<'a>
    abstract ReduceIn: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract ReduceOut: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract Reduce: Monoid<'a> -> Scalar<'a>
    abstract T: Matrix<'a>
    // abstract Kronecker

    abstract Mask: Mask2D option
    abstract Complemented: Mask2D option

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (@.) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (@.) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y

and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>(length: int) =
    abstract Length: int
    default this.Length = length

    abstract Clear: unit -> unit
    abstract Extract: Mask1D option -> Vector<'a>
    abstract Extract: int -> Scalar<'a>
    abstract Assign: Mask1D option * Vector<'a> -> unit
    abstract Assign: int * Scalar<'a> -> unit
    abstract Assign: Mask1D option * Scalar<'a> -> unit
    // abstract Dup
    // abstract Resize
    // abstrct Clear
    // abstract NNZ
    // abstract Tuples: {| Indices: int[]; Values: 'a[] |}

    abstract Vxm: Matrix<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Vector<'a> -> Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Prune: Mask1D option -> UnaryOp<'a, bool> -> Vector<'a>
    abstract Reduce: Monoid<'a> -> Scalar<'a>

    abstract Mask: Mask1D option
    abstract Complemented: Mask1D option

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
