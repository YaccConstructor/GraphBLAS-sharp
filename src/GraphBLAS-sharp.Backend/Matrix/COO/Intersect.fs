namespace GraphBLAS.FSharp.Backend.Matrix.COO

open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ClContextExtensions

module internal Intersect =
    let findKeysIntersection (clContext: ClContext) workGroupSize =

        let findIntersection =
            <@ fun (ndRange: Range1D) (leftNNZ: int) (rightNNZ: int) (leftRows: ClArray<int>) (leftColumns: ClArray<int>) (rightRows: ClArray<int>) (rightColumns: ClArray<int>) (bitmap: ClArray<int>) ->

                let gid = ndRange.GlobalID0
                let bitmapSize = min leftNNZ rightNNZ

                if gid < bitmapSize then

                    let index: uint64 = ((uint64 leftRows.[gid]) <<< 32) ||| (uint64 leftColumns.[gid])

                    let intersect = (%Search.Bin.existsByKey2D) bitmapSize index rightRows rightColumns

                    if intersect then
                        bitmap.[gid] <- 1
                    else
                        bitmap.[gid] <- 0 @>

        let kernel =
            clContext.Compile <| findIntersection

        fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix.COO<'a>) (rightMatrix: ClMatrix.COO<'b>) ->

            let bitmapSize = leftMatrix.NNZ

            let bitmap = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, bitmapSize)

            let ndRange = Range1D.CreateValid(bitmapSize, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            leftMatrix.NNZ
                            rightMatrix.NNZ
                            leftMatrix.Rows
                            leftMatrix.Columns
                            rightMatrix.Rows
                            rightMatrix.Columns
                            bitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            bitmap
