namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open GlobalContext
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

module internal Toolbox =

    let internal workGroupSize = 256
    let internal workSize n =
        let m = n - 1
        m - m % workGroupSize + workGroupSize

    let rec internal prefixSum
        (inputArray: int[]) =

        let outputArray = Array.zeroCreate inputArray.Length

        if inputArray.Length = 1 then
            let fillOutputArray =
                <@
                    fun (ndRange: _1D)
                        (inputArrayBuffer: int[])
                        (outputArrayBuffer: int[]) ->

                        let i = ndRange.GlobalID0
                        outputArrayBuffer.[i] <- inputArrayBuffer.[i]
                @>

            opencl {
                let binder kernelP =
                    let ndRange = _1D(outputArray.Length)
                    kernelP
                        ndRange
                        inputArray
                        outputArray
                do! RunCommand fillOutputArray binder
                return outputArray
            }
        else
            let intermediateArray = Array.zeroCreate ((inputArray.Length + 1) / 2)
            let inputArrayLength = inputArray.Length
            let intermediateArrayLength = intermediateArray.Length

            let fillIntermediateArray =
                <@
                    fun (ndRange: _1D)
                        (inputArrayBuffer: int[])
                        (intermediateArrayBuffer: int[]) ->

                        let i = ndRange.GlobalID0
                        if i < intermediateArrayLength then
                            if 2 * i + 1 < inputArrayLength then
                                intermediateArrayBuffer.[i] <- inputArrayBuffer.[2 * i] + inputArrayBuffer.[2 * i + 1]
                            else intermediateArrayBuffer.[i] <- inputArrayBuffer.[2 * i]
                @>

            let fillIntermediateArray =
                opencl {
                    let binder kernelP =
                        let ndRange = _1D(workSize intermediateArray.Length, workGroupSize)
                        kernelP
                            ndRange
                            inputArray
                            intermediateArray
                    do! RunCommand fillIntermediateArray binder
                }

            let fillOutputArray =
                <@
                    fun (ndRange: _1D)
                        (auxiliaryPrefixSumArrayBuffer: int[])
                        (inputArrayBuffer: int[])
                        (outputArrayBuffer: int[]) ->

                        let i = ndRange.GlobalID0
                        if i < inputArrayLength then
                            let j = (i - 1) / 2
                            if i % 2 = 0 then
                                if i = 0 then outputArrayBuffer.[i] <- inputArrayBuffer.[i]
                                else outputArrayBuffer.[i] <- auxiliaryPrefixSumArrayBuffer.[j] + inputArrayBuffer.[i]
                            else outputArrayBuffer.[i] <- auxiliaryPrefixSumArrayBuffer.[j]
                @>

            opencl {
                do! fillIntermediateArray
                let! auxiliaryPrefixSumArray = prefixSum intermediateArray

                let binder kernelP =
                    let ndRange = _1D(workSize inputArray.Length, workGroupSize)
                    kernelP
                        ndRange
                        auxiliaryPrefixSumArray
                        inputArray
                        outputArray
                do! RunCommand fillOutputArray binder

                return outputArray
            }

    let internal prefixSum2
        (inputArray: int[]) =

        let firstIntermediateArray = Array.copy inputArray
        let secondIntermediateArray = Array.copy inputArray
        let outputArrayLength = firstIntermediateArray.Length

        let updateResult =
            <@
                fun (ndRange: _1D)
                    (offset: int)
                    (firstIntermediateArrayBuffer: int[])
                    (secondIntermediateArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0
                    if i < outputArrayLength then
                        if i < offset then firstIntermediateArrayBuffer.[i] <- secondIntermediateArrayBuffer.[i]
                        else firstIntermediateArrayBuffer.[i] <- secondIntermediateArrayBuffer.[i] + secondIntermediateArrayBuffer.[i - offset]
            @>

        let binder offset firstIntermediateArray secondIntermediateArray kernelP =
            let ndRange = _1D(workSize outputArrayLength, workGroupSize)
            kernelP
                ndRange
                offset
                firstIntermediateArray
                secondIntermediateArray

        let swap (a, b) = (b, a)
        let mutable arrays = firstIntermediateArray, secondIntermediateArray

        opencl {

            let mutable offset = 1
            while offset < outputArrayLength do
                arrays <- swap arrays
                do! RunCommand updateResult <| (binder offset <|| arrays)
                offset <- offset * 2

            return (fst arrays)
        }

    let rec internal prefixSum3
        (inputArray: int[]) =

        let outputArray = Array.copy inputArray
        let outputArrayLength = outputArray.Length
        let workGroupSize = workGroupSize

        let scan =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: int[])
                    (resultLength: int)
                    (verticesBuffer: int[]) ->

                    let resultLocalBuffer = localArray<int> workGroupSize
                    let i = ndRange.GlobalID0
                    let localID = ndRange.LocalID0

                    if i < resultLength then resultLocalBuffer.[localID] <-  resultBuffer.[i] else resultLocalBuffer.[localID] <- 0

                    let mutable step = 2
                    while step <= workGroupSize do
                        barrier ()
                        if localID < workGroupSize / step then
                            let i = step * (localID + 1) - 1
                            resultLocalBuffer.[i] <- resultLocalBuffer.[i] + resultLocalBuffer.[i - (step >>> 1)]
                        step <- step <<< 1
                    barrier ()

                    if localID = workGroupSize - 1 then
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

                    if i < resultLength then resultBuffer.[i] <- resultLocalBuffer.[localID]
            @>

        let scan array length vertices =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize length, workGroupSize)
                    kernelP
                        ndRange
                        array
                        length
                        vertices
                do! RunCommand scan binder
            }

        let update =
            <@
                fun (ndRange: _1D)
                    (resultBuffer: int[])
                    (resultLength: int)
                    (verticesBuffer: int[])
                    (bunchLength: int) ->

                    let i = ndRange.GlobalID0
                    if i < resultLength then
                        resultBuffer.[i] <- resultBuffer.[i] + verticesBuffer.[i / bunchLength]
            @>

        let update vertices depth =
            opencl {
                let binder kernelP =
                    let ndRange = _1D(workSize outputArrayLength, workGroupSize)
                    kernelP
                        ndRange
                        outputArray
                        outputArrayLength
                        vertices
                        depth
                do! RunCommand update binder
            }

        let firstVertices = Array.zeroCreate <| (workSize outputArrayLength) / workGroupSize
        let secondVertices = Array.zeroCreate <| (workSize outputArrayLength) / workGroupSize
        let mutable verticesArrays = firstVertices, secondVertices
        let swap (a, b) = (b, a)

        opencl {
            do! scan outputArray outputArrayLength <| fst verticesArrays

            let mutable verticesLength = (outputArrayLength - 1) / workGroupSize + 1
            let mutable bunchLength = workGroupSize
            while verticesLength > 1 do
                do! scan (fst verticesArrays) verticesLength (snd verticesArrays)
                do! update (fst verticesArrays) bunchLength

                bunchLength <- bunchLength * workGroupSize
                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            return outputArray
        }
