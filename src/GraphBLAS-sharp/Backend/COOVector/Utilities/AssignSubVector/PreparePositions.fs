namespace GraphBLAS.FSharp.Backend.COOVector.Utilities.AssignSubVector

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

[<AutoOpen>]
module internal PreparePositions =
    let preparePositions (allIndices: int []) (rawPositions: int []) : ClTask<int []> =
        opencl {
            let length = allIndices.Length

            let preparePositions =
                <@ fun (ndRange: Range1D) (allIndicesBuffer: int []) (rawPositionsBuffer: int []) ->

                    let i = ndRange.GlobalID0

                    if i < length - 1
                       && allIndicesBuffer.[i] = allIndicesBuffer.[i + 1] then
                        rawPositionsBuffer.[i] <- 0 @>

            do!
                runCommand preparePositions
                <| fun kernelPrepare ->
                    let ndRange =
                        Range1D(Utils.getDefaultGlobalSize (length - 1), Utils.defaultWorkGroupSize)

                    kernelPrepare ndRange allIndices rawPositions

            return rawPositions
        }
