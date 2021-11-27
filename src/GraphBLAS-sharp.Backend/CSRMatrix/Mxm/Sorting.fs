namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend

module internal Sorting =
    let runInPlace (clContext: ClContext) workGroupSize =
        fun (processor: MailboxProcessor<_>) (matrix: CSRMatrix<'a>) ->
            let sort =
                <@
                    fun (ndRange: Range1D)
                        (rowPointersBuffer: ClArray<int>)
                        (columnsBuffer: ClArray<int>)
                        (valuesBuffer: ClArray<'a>) ->

                        let i = ndRange.GlobalID0
                        let localID = ndRange.LocalID0
                        let workGroupNumber = i / workGroupSize

                        let beginIndex = rowPointersBuffer.[workGroupNumber]
                        let endIndex = rowPointersBuffer.[workGroupNumber + 1]
                        let mutable n = endIndex - beginIndex

                        while n > 1 do
                            for j in beginIndex .. beginIndex + n - 2 do
                                if columnsBuffer.[j] > columnsBuffer.[j + 1]
                                then
                                    let tmp = columnsBuffer.[j]
                                    columnsBuffer.[j] <- columnsBuffer.[j + 1]
                                    columnsBuffer.[j + 1] <- tmp
                                    let tmpValue = valuesBuffer.[j]
                                    valuesBuffer.[j] <- valuesBuffer.[j + 1]
                                    valuesBuffer.[j + 1] <- tmpValue
                            n <- n - 1
                @>

            let kernel = clContext.CreateClKernel(sort)

            let ndRange = Range1D(workGroupSize * matrix.RowCount)

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.ArgumentsSetter
                            ndRange
                            matrix.RowPointers
                            matrix.Columns
                            matrix.Values)
            )
