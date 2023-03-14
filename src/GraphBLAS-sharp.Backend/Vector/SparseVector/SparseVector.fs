namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClVector

module SparseVector =
    let map2 = Map2.run

    let map2AtLeastOne (clContext: ClContext) opAdd workGroupSize allocationMode =
        Map2AtLeastOne.run clContext (Convert.atLeastOneToOption opAdd) workGroupSize allocationMode

    let assignByMask = Map2.assignByMask

    let toDense (clContext: ClContext) workGroupSize =

        let toDense =
            <@ fun (ndRange: Range1D) length (values: ClArray<'a>) (indices: ClArray<int>) (resultArray: ClArray<'a option>) ->
                let gid = ndRange.GlobalID0

                if gid < length then
                    let index = indices.[gid]

                    resultArray.[index] <- Some values.[gid] @>

        let kernel = clContext.Compile(toDense)

        let create =
            ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector.Sparse<'a>) ->
            let resultVector =
                create processor allocationMode vector.Size

            let ndRange =
                Range1D.CreateValid(vector.Indices.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc ndRange vector.Indices.Length vector.Values vector.Indices resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let reduce<'a when 'a: struct> (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) =

        let reduce =
            Reduce.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector.Sparse<'a>) -> reduce processor vector.Values
