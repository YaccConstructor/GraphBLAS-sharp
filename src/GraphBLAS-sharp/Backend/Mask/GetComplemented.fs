namespace GraphBLAS.FSharp.Backend.Mask

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open Brahma.OpenCL

module internal GetComplemented =
    let mask1D (mask: Mask1D) = opencl {
        let size = mask.Size
        let nnz = mask.Indices.Length

        let bitmap = Array.create size 1
        let getComplementedBitmap =
            <@
                fun (range: _1D)
                    (maskIndices: int[])
                    (bitmap: int[]) ->

                    let gid = range.GlobalID0

                    if gid < nnz then
                        let maskIdx = maskIndices.[gid]
                        bitmap.[maskIdx] <- 0
            @>

        do! RunCommand getComplementedBitmap <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize nnz, Utils.defaultWorkGroupSize)
            <| mask.Indices
            <| bitmap

        let! (positions, _) = PrefixSum.runExclude bitmap

        let complementedIndices = Array.zeroCreate<int> (size - nnz)
        let setPosotions =
            <@
                fun (range: _1D)
                    (positions: int[])
                    (bitmap: int[])
                    (complementedIndices: int[]) ->

                    let gid = range.GlobalID0

                    if gid < size && bitmap.[gid] = 1 then
                        complementedIndices.[positions.[gid]] <- gid
            @>

        do! RunCommand setPosotions <| fun kernelPrepare ->
            kernelPrepare
            <| _1D(Utils.getDefaultGlobalSize size, Utils.defaultWorkGroupSize)
            <| positions
            <| bitmap
            <| complementedIndices

        return Mask1D(complementedIndices, size, not mask.IsComplemented)
    }
