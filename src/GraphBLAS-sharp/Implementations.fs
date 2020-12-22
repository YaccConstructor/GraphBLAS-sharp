namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open GlobalContext
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

type CSRFormat<'a> = {
    Values: 'a[]
    Columns: int[]
    RowPointers: int[]
    ColumnCount: int
}
with
    static member CreateEmpty<'a>() = {
        Values = Array.zeroCreate<'a> 0
        Columns = Array.zeroCreate<int> 0
        RowPointers = Array.zeroCreate<int> 0
        ColumnCount = 0
    }

type CSRMatrix<'a when 'a : struct and 'a : equality>(csrTuples: CSRFormat<'a>) =
    inherit Matrix<'a>(csrTuples.RowPointers.Length - 1, csrTuples.ColumnCount)

    let rowCount = base.RowCount
    let columnCount = base.ColumnCount

    let spMV (vector: DenseVector<'a>) (mask: Mask1D) (semiring: Semiring<'a>) : Vector<'a> =
        let csrMatrixRowCount = rowCount
        let csrMatrixColumnCount = columnCount
        let vectorLength = vector.Length
        if csrMatrixColumnCount <> vectorLength then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" csrMatrixColumnCount vectorLength)

        let (BinaryOp plus) = semiring.PlusMonoid.Append
        let (BinaryOp mult) = semiring.Times

        let resultVector = Array.zeroCreate<'a> csrMatrixRowCount
        let command =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: 'a[])
                    (csrValuesBuffer: 'a[])
                    (csrColumnsBuffer: int[])
                    (csrRowPointersBuffer: int[])
                    (vectorBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0
                    let mutable localResultBuffer = resultBuffer.[i]
                    for k in csrRowPointersBuffer.[i] .. csrRowPointersBuffer.[i + 1] - 1 do
                        localResultBuffer <- (%plus) localResultBuffer
                            ((%mult) csrValuesBuffer.[k] vectorBuffer.[csrColumnsBuffer.[k]])
                    resultBuffer.[i] <- localResultBuffer
            @>

        let ndRange = _1D(csrMatrixRowCount)
        let binder = fun kernelPrepare ->
            kernelPrepare
                ndRange
                resultVector
                csrTuples.Values
                csrTuples.Columns
                csrTuples.RowPointers
                vector.Values

        let eval = opencl {
            do! RunCommand command binder
            return! ToHost resultVector
        }

        upcast DenseVector(oclContext.RunSync eval, semiring.PlusMonoid)

    new(rows: int[], columns: int[], values: 'a[]) = CSRMatrix(CSRFormat.CreateEmpty())

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.Columns
    member this.RowPointers = csrTuples.RowPointers

    override this.Extract (mask: Mask2D option) : Matrix<'a> = failwith "Not Implemented"
    override this.Extract (colMask: Mask1D option * int) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (rowMask: int * Mask1D option) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (idx: int * int) : Scalar<'a> = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Matrix<'a>) : unit = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign (idx: int * int, value: Scalar<'a>) : unit = failwith "Not Implemented"
    override this.Assign (mask: Mask2D option, value: Scalar<'a>) : unit = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D option * int, value: Scalar<'a>) : unit = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D option, value: Scalar<'a>) : unit = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"
    override this.T = failwith "Not Implemented"

    override this.Mask = failwith "Not implemented"
    override this.Complemented = failwith "Not implemented"

and SparseVector<'a when 'a : struct and 'a : equality>(size: int, listOfNonzeroes: (int * 'a) list) =
    inherit Vector<'a>(size)

    override this.Clear() = failwith "Not Implemented"
    override this.Extract (mask: Mask1D option) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (idx: int) : Scalar<'a> = failwith "Not Implemented"
    override this.Assign(mask: Mask1D option, vector: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign(idx: int, Scalar (value: 'a)) : unit = failwith "Not Implemented"
    override this.Assign(mask: Mask1D option, Scalar (value: 'a)) : unit = failwith "Not Implemented"

    override this.Vxm (matrix: Matrix<'a>) (mask: Mask1D option) (semiring: Semiring<'a>) : Vector<'a> = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"

    override this.Mask = failwith "Not implemented"
    override this.Complemented = failwith "Not implemented"

and DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[], monoid: Monoid<'a>) =
    inherit Vector<'a>(vector.Length)

    member this.Monoid = monoid
    member this.Values: 'a[] = vector

    override this.Clear() = failwith "Not Implemented"
    override this.Extract (mask: Mask1D option) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (idx: int) : Scalar<'a> = failwith "Not Implemented"
    override this.Assign(mask: Mask1D option, vector: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign(idx: int, Scalar (value: 'a)) : unit = failwith "Not Implemented"
    override this.Assign(mask: Mask1D option, Scalar (value: 'a)) : unit = failwith "Not Implemented"

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Prune a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"

    override this.Mask =
        let indices =
            [| for i in 0 .. this.Length - 1 do
                if this.Values.[i] <> this.Monoid.Zero then yield i |]
        Some <| Mask1D(indices, this.Length, false)

    override this.Complemented =
        let indices =
            [| for i in 0 .. this.Length - 1 do
                if this.Values.[i] <> this.Monoid.Zero then yield i |]
        Some <| Mask1D(indices, this.Length, true)
