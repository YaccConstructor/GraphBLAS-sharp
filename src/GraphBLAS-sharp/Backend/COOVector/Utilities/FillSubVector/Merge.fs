namespace GraphBLAS.FSharp.Backend.COOVector.Utilities.FillSubVector

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common

[<AutoOpen>]
module internal Merge =
    let merge (leftIndices: int[]) (leftValues: 'a[]) (rightIndices: int[]) (scalar: 'a[]) : OpenCLEvaluation<int[] * 'a[]> = opencl {
        let workGroupSize = Utils.defaultWorkGroupSize
        let firstSide = leftValues.Length
        let secondSide = rightIndices.Length
        let sumOfSides = firstSide + secondSide

        let merge =
            <@
                fun (ndRange: _1D)
                    (firstIndicesBuffer: int[])
                    (firstValuesBuffer: 'a[])
                    (secondIndicesBuffer: int[])
                    (scalarBuffer: 'a[])
                    (allIndicesBuffer: int[])
                    (allValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    let mutable beginIdxLocal = local ()
                    let mutable endIdxLocal = local ()
                    let localID = ndRange.LocalID0
                    if localID < 2 then
                        let mutable x = localID * (workGroupSize - 1) + i - 1
                        if x >= sumOfSides then x <- sumOfSides - 1
                        let diagonalNumber = x

                        let mutable leftEdge = diagonalNumber + 1 - secondSide
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstSide - 1
                        if rightEdge > diagonalNumber then rightEdge <- diagonalNumber

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = firstIndicesBuffer.[middleIdx]
                            let secondIndex = secondIndicesBuffer.[diagonalNumber - middleIdx]
                            if firstIndex <= secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        // Here localID equals either 0 or 1
                        if localID = 0 then beginIdxLocal <- leftEdge else endIdxLocal <- leftEdge
                    barrier ()

                    let beginIdx = beginIdxLocal
                    let endIdx = endIdxLocal
                    let firstLocalLength = endIdx - beginIdx
                    let mutable x = workGroupSize - firstLocalLength
                    if endIdx = firstSide then x <- secondSide - i + localID + beginIdx
                    let secondLocalLength = x

                    //First indices are from 0 to firstLocalLength - 1 inclusive
                    //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                    let localIndices = localArray<int> workGroupSize

                    if localID < firstLocalLength then
                        localIndices.[localID] <- firstIndicesBuffer.[beginIdx + localID]
                    if localID < secondLocalLength then
                        localIndices.[firstLocalLength + localID] <- secondIndicesBuffer.[i - beginIdx]
                    barrier ()

                    if i < sumOfSides then
                        let mutable leftEdge = localID + 1 - secondLocalLength
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstLocalLength - 1
                        if rightEdge > localID then rightEdge <- localID

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = localIndices.[middleIdx]
                            let secondIndex = localIndices.[firstLocalLength + localID - middleIdx]
                            if firstIndex <= secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

                        let boundaryX = rightEdge
                        let boundaryY = localID - leftEdge

                        // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
                        let isValidX = boundaryX >= 0
                        let isValidY = boundaryY >= 0

                        let mutable fstIdx = 0
                        if isValidX then fstIdx <- localIndices.[boundaryX]

                        let mutable sndIdx = 0
                        if isValidY then sndIdx <- localIndices.[firstLocalLength + boundaryY]

                        if not isValidX || isValidY && fstIdx <= sndIdx then
                            allIndicesBuffer.[i] <- sndIdx
                            allValuesBuffer.[i] <- scalarBuffer.[0]
                        else
                            allIndicesBuffer.[i] <- fstIdx
                            allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
            @>

        let allIndices = Array.zeroCreate sumOfSides
        let allValues = Array.create sumOfSides Unchecked.defaultof<'a>

        do! RunCommand merge <| fun kernelPrepare ->
            let ndRange = _1D(Utils.getDefaultGlobalSize sumOfSides, workGroupSize)
            kernelPrepare
                ndRange
                leftIndices
                leftValues
                rightIndices
                scalar
                allIndices
                allValues

        return allIndices, allValues
    }
