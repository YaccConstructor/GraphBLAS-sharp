namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCLContext
open Helpers
open FSharp.Quotations.Evaluator

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

    let spMV (vector: Vector<'a>) (mask: Mask1D) (semiring: Semiring<'a>) : Vector<'a> =
        let csrMatrixRowCount = rowCount
        let csrMatrixColumnCount = columnCount
        let vectorLength = vector.Size
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

        let (kernel, kernelPrepare, kernelRun) = currentContext.Provider.Compile command
        let ndRange = _1D(csrMatrixRowCount)
        kernelPrepare
            ndRange
            resultVector
            csrTuples.Values
            csrTuples.Columns
            csrTuples.RowPointers
            vector.AsArray
        currentContext.CommandQueue.Add (kernelRun ()) |> ignore
        currentContext.CommandQueue.Add (resultVector.ToHost currentContext.Provider) |> ignore
        currentContext.CommandQueue.Finish () |> ignore

        upcast DenseVector(resultVector, semiring.PlusMonoid)

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.Columns
    member this.RowPointers = csrTuples.RowPointers

    override this.Item
        with get (mask: Mask2D) : Matrix<'a> = failwith "Not Implemented"
        and set (mask: Mask2D) (value: Matrix<'a>) = failwith "Not Implemented"
    override this.Item
        with get (vectorMask: Mask1D, colIdx: int) : Vector<'a> = failwith "Not Implemented"
        and set (vectorMask: Mask1D, colIdx: int) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, vectorMask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (rowIdx: int, vectorMask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (mask: Mask2D) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (vectorMask: Mask1D, colIdx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (rowIdx: int, vectorMask: Mask1D) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.Reduce a = failwith "Not Implemented"
    override this.T = failwith "Not Implemented"

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"

and SparseVector<'a when 'a : struct and 'a : equality>(size: int, listOfNonzeroes: (int * 'a) list) =
    inherit Vector<'a>(size)

    member this.AsList: (int * 'a) list = listOfNonzeroes

    override this.AsArray = failwith "Not Implemented"
    override this.Clear () = failwith "Not Implemented"

    override this.Item
        with get (mask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (idx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (idx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (mask: Mask1D) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Vxm (matrix: Matrix<'b>) (mask: Mask1D) (semiring: Semiring<'a, 'b, 'c>) : Vector<'c> = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) = failwith "Not Implemented"

    override this.VxmInplace a b c = failwith "Not Implemented"
    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"

and DenseVector<'a when 'a : struct and 'a : equality>(vector: 'a[], monoid: Monoid<'a>) =
    inherit Vector<'a>(vector.Length)

    override this.AsArray = vector
    override this.Clear () = failwith "Not Implemented"

    override this.Item
        with get (mask: Mask1D) : Vector<'a> = failwith "Not Implemented"
        and set (mask: Mask1D) (value: Vector<'a>) = failwith "Not Implemented"
    override this.Item
        with get (idx: int) : Scalar<'a> = failwith "Not Implemented"
        and set (idx: int) (value: Scalar<'a>) = failwith "Not Implemented"
    override this.Fill
        with set (mask: Mask1D) (value: Scalar<'a>) = failwith "Not Implemented"

    override this.Vxm a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b = failwith "Not Implemented"
    override this.Reduce (monoid: Monoid<'a>) =
        vector
        |> Array.reduce (QuotationEvaluator.Evaluate !> monoid.Append)
        |> Scalar

    override this.VxmInplace a b c = failwith "Not Implemented"
    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
