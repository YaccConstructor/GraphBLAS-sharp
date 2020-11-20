namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open OpenCLContext

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

    let spMV (vector: Vector<'a>) (mask: Mask1D<'a>) (semiring: Semiring<'a>) : Vector<'a> =
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

        upcast DenseVector(resultVector)

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.Columns
    member this.RowPointers = csrTuples.RowPointers

    override this.Extract (mask: Mask2D<'t>) : Matrix<'a> = failwith "Not Implemented"
    override this.Extract (colMask: Mask1D<'t> * int) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (rowMask: int * Mask1D<'t>) : Vector<'a> = failwith "Not Implemented"
    override this.Extract (idx: int * int) : Scalar<'a> = failwith "Not Implemented"

    override this.Assign (mask: Mask2D<'t>, value: Matrix<'a>) : unit = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D<'t> * int, value: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D<'t>, value: Vector<'a>) : unit = failwith "Not Implemented"
    override this.Assign (idx: int * int, value: Scalar<'a>) : unit = failwith "Not Implemented"
    override this.Assign (mask: Mask2D<'t>, value: Scalar<'a>) : unit = failwith "Not Implemented"
    override this.Assign (colMask: Mask1D<'t> * int, value: Scalar<'a>) : unit = failwith "Not Implemented"
    override this.Assign (rowMask: int * Mask1D<'t>, value: Scalar<'a>) : unit = failwith "Not Implemented"

    override this.Mxm a b c = failwith "Not Implemented"
    override this.Mxv a b c = failwith "Not Implemented"
    override this.EWiseAdd a b c = failwith "Not Implemented"
    override this.EWiseMult a b c = failwith "Not Implemented"
    override this.Apply a b  = failwith "Not Implemented"
    override this.ReduceIn a b = failwith "Not Implemented"
    override this.ReduceOut a b = failwith "Not Implemented"
    override this.T = failwith "Not Implemented"

    override this.EWiseAddInplace a b c = failwith "Not Implemented"
    override this.EWiseMultInplace a b c = failwith "Not Implemented"
    override this.ApplyInplace a b = failwith "Not Implemented"
