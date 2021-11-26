namespace GraphBLAS.FSharp.Backend.CSRMatrix.Mxm

open Brahma.FSharp.OpenCL
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend

module internal Expansion =
    let run (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (matrixLeft: CSRMatrix<'a>) (matrixRight: CSRMatrix<'a>) (resultMatrix: CSRMatrix<'a>) (times: Expr<'a -> 'a -> 'a>) ->
            let rowCount = matrixLeft.RowCount

            let expand =
                <@
                    fun (ndRange: Range1D)
                        (firstRowPointersBuffer: ClArray<int>)
                        (firstColumnsBuffer: ClArray<int>)
                        (firstValuesBuffer: ClArray<'a>)
                        (secondRowPointersBuffer: ClArray<int>)
                        (secondColumnsBuffer: ClArray<int>)
                        (secondValuesBuffer: ClArray<'a>)
                        (resultRowPointersBuffer: ClArray<int>)
                        (resultColumnsBuffer: ClArray<int>)
                        (resultValuesBuffer: ClArray<'a>) ->

                        let i = ndRange.GlobalID0

                        if i < rowCount then
                            let mutable bound = resultRowPointersBuffer.[i]
                            for k in firstRowPointersBuffer.[i] .. firstRowPointersBuffer.[i + 1] - 1 do
                                let colIdx = firstColumnsBuffer.[k]
                                let value = firstValuesBuffer.[k]
                                for j in secondRowPointersBuffer.[colIdx] .. secondRowPointersBuffer.[colIdx + 1] - 1 do
                                    resultColumnsBuffer.[bound] <- secondColumnsBuffer.[j]
                                    resultValuesBuffer.[bound] <- (%times) value secondValuesBuffer.[j]
                                    bound <- bound + 1
                @>

            let kernel = clContext.CreateClKernel(expand)

            let ndRange = Range1D.CreateValid(matrixLeft.RowCount, workGroupSize)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.SetArguments
                            ndRange
                            matrixLeft.RowPointers
                            matrixLeft.Columns
                            matrixLeft.Values
                            matrixRight.RowPointers
                            matrixRight.Columns
                            matrixRight.Values
                            resultMatrix.RowPointers
                            resultMatrix.Columns
                            resultMatrix.Values)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultMatrix
