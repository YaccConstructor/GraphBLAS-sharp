namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

module internal rec PrefixSum =
    let runInplace (inputArray: int[]) (totalSum: int[]) = opencl {
        let workGroupSize = Utils.workGroupSize

        let firstVertices = Array.zeroCreate <| (inputArray.Length - 1) / workGroupSize + 1
        let secondVertices = Array.zeroCreate <| (firstVertices.Length - 1) / workGroupSize + 1
        let mutable verticesArrays = firstVertices, secondVertices
        let swap (a, b) = (b, a)

        let mutable verticesLength = (inputArray.Length - 1) / workGroupSize + 1
        let mutable bunchLength = workGroupSize

        do! scan inputArray inputArray.Length (fst verticesArrays) verticesLength totalSum
        while verticesLength > 1 do
            let fstVertices = fst verticesArrays
            let sndVertices = snd verticesArrays
            do! scan fstVertices verticesLength sndVertices ((verticesLength - 1) / workGroupSize + 1) totalSum
            do! update inputArray inputArray.Length fstVertices bunchLength

            bunchLength <- bunchLength * workGroupSize
            verticesArrays <- swap verticesArrays
            verticesLength <- (verticesLength - 1) / workGroupSize + 1
    }

    let private scan (inputArray: int[]) (inputArrayLength: int) (vertices: int[]) (verticesLength: int) (totalSum: int[]) = opencl {
        let workGroupSize = Utils.workGroupSize

        let scan =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: int[])
                    (verticesBuffer: int[])
                    (totalSumBuffer: int[]) ->

                    let resultLocalBuffer = localArray<int> workGroupSize
                    let i = ndRange.GlobalID0
                    let localID = ndRange.LocalID0

                    if i < inputArrayLength then resultLocalBuffer.[localID] <- resultBuffer.[i] else resultLocalBuffer.[localID] <- 0

                    let mutable step = 2
                    while step <= workGroupSize do
                        barrier ()
                        if localID < workGroupSize / step then
                            let i = step * (localID + 1) - 1
                            resultLocalBuffer.[i] <- resultLocalBuffer.[i] + resultLocalBuffer.[i - (step >>> 1)]
                        step <- step <<< 1
                    barrier ()

                    if localID = workGroupSize - 1 then
                        if verticesLength <= 1 && localID = i then totalSumBuffer.[0] <- resultLocalBuffer.[localID]
                        verticesBuffer.[i / workGroupSize] <- resultLocalBuffer.[localID]
                        resultLocalBuffer.[localID] <- 0

                    step <- workGroupSize
                    while step > 1 do
                        barrier ()
                        if localID < workGroupSize / step then
                            let i = step * (localID + 1) - 1
                            let j = i - (step >>> 1)

                            let tmp = resultLocalBuffer.[i]
                            resultLocalBuffer.[i] <- resultLocalBuffer.[i] + resultLocalBuffer.[j]
                            resultLocalBuffer.[j] <- tmp
                        step <- step >>> 1
                    barrier ()

                    if i < inputArrayLength then resultBuffer.[i] <- resultLocalBuffer.[localID]
            @>

        do! RunCommand scan <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize inputArrayLength, workGroupSize)
            kernelPrepare
                ndRange
                inputArray
                vertices
                totalSum
    }

    let private update (inputArray: int[]) (inputArrayLength: int) (vertices: int[]) (bunchLength: int) = opencl {
        let workGroupSize = Utils.workGroupSize

        let update =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: int[])
                    (verticesBuffer: int[]) ->

                    let i = ndRange.GlobalID0 + bunchLength
                    if i < inputArrayLength then
                        resultBuffer.[i] <- resultBuffer.[i] + verticesBuffer.[i / bunchLength]
            @>

        do! RunCommand update <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize inputArrayLength - bunchLength, workGroupSize)
            kernelPrepare
                ndRange
                inputArray
                vertices
    }
