namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions

type CSRMatrix<'a>(denseMatrix: 'a[,]) =
    inherit Matrix<'a>()

    let rowsCount = denseMatrix |> Array2D.length1
    let columnsCount = denseMatrix |> Array2D.length2

    let convertedMatrix =
        [for i in 0 .. rowsCount - 1 -> denseMatrix.[i, *] |> List.ofArray]
        |> List.map (fun row ->
            row
            |> List.mapi (fun i x -> (x, i))
            // |> List.filter (fun pair -> fst pair |> abs > System.Double.Epsilon)
            )
        |> List.fold (fun (rowPtrs, valueInx) row ->
            ((rowPtrs.Head + row.Length) :: rowPtrs), valueInx @ row) ([0], [])

    member this.Values = convertedMatrix |> (snd >> List.unzip >> fst) |> List.toArray
    member this.Columns = convertedMatrix |> (snd >> List.unzip >> snd) |> List.toArray
    member this.RowPointers = convertedMatrix |> fst |> List.rev |> List.toArray

    override this.RowCount = this.RowPointers.Length - 1
    override this.ColumnCount = columnsCount

    override this.Mxv(vector: Vector<'a>) (context: ComputationalContext<'a>) =
        let csrMatrixRowCount = this.RowCount
        let csrMatrixColumnCount = this.ColumnCount
        let vectorLength = vector.Length
        if csrMatrixColumnCount <> vectorLength then
            invalidArg
                "vector"
                (sprintf "Argument has invalid dimension. Need %i, but given %i" csrMatrixColumnCount vectorLength)

        let plus = context.Semiring.Addition
        let mult = context.Semiring.Multiplication

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

        let (kernel, kernelPrepare, kernelRun) = context.OCLContext.Provider.Compile command
        let ndRange = _1D(csrMatrixRowCount)
        kernelPrepare
            ndRange
            resultVector
            this.Values
            this.Columns
            this.RowPointers
            vector.AsArray
        context.OCLContext.CommandQueue.Add (kernelRun ()) |> ignore
        context.OCLContext.CommandQueue.Add (resultVector.ToHost context.OCLContext.Provider) |> ignore
        context.OCLContext.CommandQueue.Finish () |> ignore

        upcast DenseVector(resultVector)

module CSRMatrix =
    let ofDense (matrix: 'a[,]) = CSRMatrix(matrix)
    let rowCount (matrix: CSRMatrix<'a>) = matrix.RowCount
    let columnCount (matrix: CSRMatrix<'a>) = matrix.ColumnCount
    let nnz (matrix: CSRMatrix<'a>) = matrix.RowPointers.[matrix.RowPointers.Length - 1]

    let s = CSRMatrix(Array2D.map StandardSemiring (array2D [[1.;2.;3.];[1.;2.;3.];[1.;2.;3.]]))
