namespace GraphBLAS.FSharp.Backend.Mask

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

module internal GetComplemented =
    let mask1D (mask: Mask1D) =
        opencl {
            if mask.Indices.Length = 0 then
                let indices = Array.init mask.Size id
                return Mask1D(indices, mask.Size, not mask.IsComplemented)
            elif mask.Indices.Length = mask.Size then
                return Mask1D([||], mask.Size, not mask.IsComplemented)
            else
                let size = mask.Size
                let nnz = mask.Indices.Length

                let bitmap = Array.create size 1

                let getComplementedBitmap =
                    <@ fun (range: Range1D) (maskIndices: int []) (bitmap: int []) ->

                        let gid = range.GlobalID0

                        if gid < nnz then
                            let maskIdx = maskIndices.[gid]
                            bitmap.[maskIdx] <- 0 @>

                do!
                    runCommand getComplementedBitmap
                    <| fun kernelPrepare ->
                        kernelPrepare
                        <| Range1D(Utils.getDefaultGlobalSize nnz, Utils.defaultWorkGroupSize)
                        <| mask.Indices
                        <| bitmap

                let! (positions, _) = PrefixSum.runExclude bitmap

                let complementedIndices = Array.zeroCreate<int> (size - nnz)

                let setPosotions =
                    <@ fun (range: Range1D) (positions: int []) (bitmap: int []) (complementedIndices: int []) ->

                        let gid = range.GlobalID0

                        if gid < size && bitmap.[gid] = 1 then
                            complementedIndices.[positions.[gid]] <- gid @>

                do!
                    runCommand setPosotions
                    <| fun kernelPrepare ->
                        kernelPrepare
                        <| Range1D(Utils.getDefaultGlobalSize size, Utils.defaultWorkGroupSize)
                        <| positions
                        <| bitmap
                        <| complementedIndices

                return Mask1D(complementedIndices, size, not mask.IsComplemented)
        }
