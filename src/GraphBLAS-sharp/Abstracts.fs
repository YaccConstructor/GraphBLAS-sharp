namespace GraphBLAS.FSharp

<<<<<<< HEAD
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
=======
>>>>>>> e5c9ed5c70dc7da1eda621582663215739e51536
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<AbstractClass>]
type Matrix<'a when 'a : struct and 'a : equality>(nrow: int, ncol: int) =
    abstract RowCount: int
    abstract ColumnCount: int
    default this.RowCount = nrow
    default this.ColumnCount = ncol

<<<<<<< HEAD
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
    abstract EWiseAdd: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract EWiseMult: Matrix<'a> -> Mask2D option -> Semiring<'a> -> Matrix<'a>
    abstract Apply: Mask2D option -> UnaryOp<'a, 'b> -> Matrix<'b>
    abstract ReduceIn: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract ReduceOut: Mask1D option -> Monoid<'a> -> Vector<'a>
    abstract Reduce: Monoid<'a> -> Scalar<'a>
    abstract T: Matrix<'a>
=======
    abstract Clear: unit -> OpenCLEvaluation<unit>
    abstract Copy: unit -> OpenCLEvaluation<Matrix<'a>>
    abstract Resize: int -> int -> OpenCLEvaluation<Matrix<'a>>
    abstract GetNNZ: unit -> OpenCLEvaluation<int>
    abstract GetTuples: unit -> OpenCLEvaluation<{| Rows: int[]; Columns: int[]; Values: 'a[] |}>
    abstract GetMask: ?isComplemented: bool -> OpenCLEvaluation<Mask2D option>

    abstract Extract: Mask2D option -> OpenCLEvaluation<Matrix<'a>>
    abstract Extract: (Mask1D option * int) -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: (int * Mask1D option) -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: (int * int) -> OpenCLEvaluation<Scalar<'a>>
    abstract Assign: Mask2D option * Matrix<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (Mask1D option * int) * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * Mask1D option) * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * int) * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: Mask2D option * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (Mask1D option * int) * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: (int * Mask1D option) * Scalar<'a> -> OpenCLEvaluation<unit>

    abstract Mxm: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract Mxv: Vector<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseAdd: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract EWiseMult: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
    abstract Apply: Mask2D option -> UnaryOp<'a, 'b> -> OpenCLEvaluation<Matrix<'b>>
    abstract Prune: Mask2D option -> UnaryOp<'a, bool> -> OpenCLEvaluation<Matrix<'a>>
    abstract ReduceIn: Mask1D option -> Monoid<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract ReduceOut: Mask1D option -> Monoid<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract Reduce: Monoid<'a> -> OpenCLEvaluation<Scalar<'a>>
    abstract Transpose: unit -> OpenCLEvaluation<Matrix<'a>>
    abstract Kronecker: Matrix<'a> -> Mask2D option -> Semiring<'a> -> OpenCLEvaluation<Matrix<'a>>
>>>>>>> e5c9ed5c70dc7da1eda621582663215739e51536

    static member inline (+) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseAdd y
    static member inline (*) (x: Matrix<'a>, y: Matrix<'a>) = x.EWiseMult y
    static member inline (@.) (x: Matrix<'a>, y: Matrix<'a>) = x.Mxm y
    static member inline (@.) (x: Matrix<'a>, y: Vector<'a>) = x.Mxv y


and [<AbstractClass>] Vector<'a when 'a : struct and 'a : equality>(size: int) =
    abstract Size: int
    default this.Size = size

    abstract Clear: unit -> OpenCLEvaluation<unit>
    abstract Copy: unit -> OpenCLEvaluation<Vector<'a>>
    abstract Resize: int -> OpenCLEvaluation<Vector<'a>>
    abstract GetNNZ: unit -> OpenCLEvaluation<int>
    abstract GetTuples: unit -> OpenCLEvaluation<{| Indices: int[]; Values: 'a[] |}>
    abstract GetMask: ?isComplemented: bool -> OpenCLEvaluation<Mask1D option>

    abstract Extract: Mask1D option -> OpenCLEvaluation<Vector<'a>>
    abstract Extract: int -> OpenCLEvaluation<Scalar<'a>>
    abstract Assign: Mask1D option * Vector<'a> -> OpenCLEvaluation<unit>
    abstract Assign: int * Scalar<'a> -> OpenCLEvaluation<unit>
    abstract Assign: Mask1D option * Scalar<'a> -> OpenCLEvaluation<unit>

<<<<<<< HEAD
    abstract Vxm: Matrix<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseAdd: Vector<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract EWiseMult: Vector<'a> -> Mask1D option -> Semiring<'a> -> Vector<'a>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> Vector<'b>
    abstract Reduce: Monoid<'a> -> Scalar<'a>
=======
    abstract Vxm: Matrix<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseAdd: Vector<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract EWiseMult: Vector<'a> -> Mask1D option -> Semiring<'a> -> OpenCLEvaluation<Vector<'a>>
    abstract Apply: Mask1D option -> UnaryOp<'a, 'b> -> OpenCLEvaluation<Vector<'b>>
    abstract Prune: Mask1D option -> UnaryOp<'a, bool> -> OpenCLEvaluation<Vector<'a>>
    abstract Reduce: Monoid<'a> -> OpenCLEvaluation<Scalar<'a>>
>>>>>>> e5c9ed5c70dc7da1eda621582663215739e51536

    static member inline (+) (x: Vector<'a>, y: Vector<'a>) = x.EWiseAdd y
    static member inline (*) (x: Vector<'a>, y: Vector<'a>) = x.EWiseMult y
    static member inline (@.) (x: Vector<'a>, y: Matrix<'a>) = x.Vxm y


and Mask1D(indices: int[], size: int, isComplemented: bool) =
    member this.Indices = indices
    member this.Size = size
    member this.IsComplemented = isComplemented

<<<<<<< HEAD
    member this.Item
        with get (idx: int) : bool =
            this.Indices
            |> Array.contains idx
            |> (<>) this.IsComplemented
=======
>>>>>>> e5c9ed5c70dc7da1eda621582663215739e51536

and Mask2D(rowIndices: int[], columnIndices: int[], rowCount: int, columnCount: int, isComplemented: bool) =
    member this.RowIndices = rowIndices
    member this.ColumnIndices = columnIndices
    member this.RowCount = rowCount
    member this.ColumnCount = columnCount
    member this.IsComplemented = isComplemented
<<<<<<< HEAD

    member this.Item
        with get (rowIdx: int, colIdx: int) : bool =
            (this.Rows, this.Columns)
            ||> Array.zip
            |> Array.contains (rowIdx, colIdx)
            |> (<>) this.IsComplemented
=======
>>>>>>> e5c9ed5c70dc7da1eda621582663215739e51536
