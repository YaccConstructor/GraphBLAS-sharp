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
    static member ZeroCreate<'a>() = {
        Values = Array.zeroCreate<'a> 0
        Columns = Array.zeroCreate<int> 0
        RowPointers = Array.zeroCreate<int> 0
        ColumnCount = 0
    }

type CSRMatrix<'a>(csrTuples: CSRFormat<'a>) =
    inherit Matrix<'a>()

    let rowCount = csrTuples.RowPointers.Length - 1
    let columnCount = csrTuples.ColumnCount

    let spMV (vector: Vector<'a>) (mask: Mask1D option) (context: Semiring<'a>) : Vector<'a> =
        let csrMatrixRowCount = rowCount
        let csrMatrixColumnCount = columnCount
        let vectorLength = vector.Length
        if csrMatrixColumnCount <> vectorLength then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" csrMatrixColumnCount vectorLength)

        let plus = !> context.PlusMonoid.Append
        let mult = !> context.Times

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

    new() = CSRMatrix(CSRFormat.ZeroCreate())

    member this.Values = csrTuples.Values
    member this.Columns = csrTuples.Columns
    member this.RowPointers = csrTuples.RowPointers

    override this.RowCount = rowCount
    override this.ColumnCount = columnCount

    override this.Item
        with get (mask: Mask2D option) : Matrix<'a> = upcast CSRMatrix<'a>()
        and set (mask: Mask2D option) (value: Matrix<'a>) = ()
    override this.Item
        with get (vectorMask: Mask1D option, colIdx: int) : Vector<'a> = upcast DenseVector<'a>()
        and set (vectorMask: Mask1D option, colIdx: int) (value: Vector<'a>) = ()
    override this.Item
        with get (rowIdx: int, vectorMask: Mask1D option) : Vector<'a> = upcast DenseVector<'a>()
        and set (rowIdx: int, vectorMask: Mask1D option) (value: Vector<'a>) = ()
    override this.Item
        with get (rowIdx: int, colIdx: int) : Scalar<'a> = Scalar Unchecked.defaultof<'a>
        and set (rowIdx: int, colIdx: int) (value: Scalar<'a>) = ()

    override this.Item
        with set (mask: Mask2D option) (value: Scalar<'a>) = ()
    override this.Item
        with set (vectorMask: Mask1D option, colIdx: int) (value: Scalar<'a>) = ()
    override this.Item
        with set (rowIdx: int, vectorMask: Mask1D option) (value: Scalar<'a>) = ()

    override this.Mxm a b c = upcast CSRMatrix<'a>()
    override this.Mxv a b c = upcast DenseVector<'a>()
    override this.EWiseAdd a b c = upcast CSRMatrix<'a>()
    override this.EWiseMult a b c = upcast CSRMatrix<'a>()
    override this.Apply<'b> a b  = upcast CSRMatrix<'b>()
    override this.ReduceIn a b = upcast DenseVector<'a>()
    override this.ReduceOut a b = upcast DenseVector<'a>()
    override this.T = upcast CSRMatrix<'a>()

    override this.EWiseAddInplace a b c = ()
    override this.EWiseMultInplace a b c = ()
    override this.ApplyInplace a b = ()
