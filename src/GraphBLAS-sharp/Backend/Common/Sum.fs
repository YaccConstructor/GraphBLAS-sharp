namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Microsoft.FSharp.Quotations

module internal rec Sum =
    let run (inputArray: 'a []) (plus: Expr<'a -> 'a -> 'a>) (zero: 'a) =
        if inputArray.Length = 0 then
            opencl {
                let result = [| zero |]

                let bruh =
                    <@ fun (range: _1D) (array: 'a []) ->
                        let mutable a = 0
                        a <- 0 @>

                do!
                    RunCommand bruh
                    <| fun kernelPrepare -> kernelPrepare <| _1D (64, 64) <| result

                return result
            }
        else
            runNotEmpty inputArray plus zero

    let private runNotEmpty (inputArray: 'a []) (plus: Expr<'a -> 'a -> 'a>) (zero: 'a) =
        opencl {
            let workGroupSize = Utils.defaultWorkGroupSize

            let firstVertices =
                Array.zeroCreate
                <| (inputArray.Length - 1) / workGroupSize + 1

            let secondVertices =
                Array.zeroCreate
                <| (firstVertices.Length - 1) / workGroupSize + 1

            let mutable verticesArrays = firstVertices, secondVertices
            let swap (a, b) = (b, a)

            let mutable verticesLength = firstVertices.Length

            do! scan inputArray inputArray.Length (fst verticesArrays) plus zero

            while verticesLength > workGroupSize do
                let fstVertices = fst verticesArrays
                let sndVertices = snd verticesArrays
                do! scan fstVertices verticesLength sndVertices plus zero

                verticesArrays <- swap verticesArrays
                verticesLength <- (verticesLength - 1) / workGroupSize + 1

            let result = Array.create 1 zero

            let fstVertices = fst verticesArrays
            do! scan fstVertices verticesLength result plus zero

            return result
        }

    let private scan
        (inputArray: 'a [])
        (inputArrayLength: int)
        (vertices: 'a [])
        (plus: Expr<'a -> 'a -> 'a>)
        (zero: 'a)
        =
        opencl {
            let workGroupSize = Utils.defaultWorkGroupSize

            let scan =
                <@ fun (ndRange: _1D) (resultBuffer: 'a []) (verticesBuffer: 'a []) ->

                    let i = ndRange.GlobalID0
                    let localID = ndRange.LocalID0

                    let resultLocalBuffer = localArray<'a> workGroupSize

                    if i < inputArrayLength then
                        resultLocalBuffer.[localID] <- resultBuffer.[i]
                    else
                        resultLocalBuffer.[localID] <- zero

                    let mutable step = 2

                    while step <= workGroupSize do
                        barrier ()

                        if localID < workGroupSize / step then
                            let i = step * (localID + 1) - 1
                            resultLocalBuffer.[i] <- (%plus) resultLocalBuffer.[i] resultLocalBuffer.[i - (step >>> 1)]

                        step <- step <<< 1

                    barrier ()

                    if localID = workGroupSize - 1 then
                        verticesBuffer.[i / workGroupSize] <- resultLocalBuffer.[localID] @>

            do!
                RunCommand scan
                <| fun kernelPrepare ->
                    let ndRange =
                        _1D (Utils.getDefaultGlobalSize inputArrayLength, workGroupSize)

                    kernelPrepare ndRange inputArray vertices
        }
