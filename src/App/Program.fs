open System

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

open System
open GraphBLAS.FSharp
open Microsoft.FSharp.Reflection
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Predefined

open OpenCL.Net
open GraphBLAS.FSharp.Backend.Common
open Brahma.FSharp.OpenCL.Core

[<EntryPoint>]
let main argv =

    let workGroupSize = 256
    let workSize n =
        let m = n - 1
        m - m % workGroupSize + workGroupSize

    // let mutable oclContext = OpenCLEvaluationContext("Intel*", DeviceType.Gpu)
    let mutable oclContext = OpenCLEvaluationContext("NVIDIA*")


    let leftVector = SparseVector(100, [|4;10;11|], [|2.;9.;8.|])
    let rightVector = SparseVector(100, [|0;1;2;3;4;5|], [|5.;4.;3.;2.;1.;0.|])

    let workflow =
        opencl {
            let! resultVector = leftVector.EWiseAdd rightVector None FloatSemiring.addMult
            return! resultVector.ToHost()
        }

    let res = oclContext.RunSync workflow
    printfn "%A" res



    // // let leftMatrix = COOMatrix(100, 100, [|0;1;2;3;4;7|], [|1;7;5;6;0;1|], [|1.;2.;-4.;4.;5.;6.|])
    // // let rightMatrix = COOMatrix(100, 100, [|0;0;1;2;3;4;7|], [|1;5;4;5;7;0;1|], [|1.;2.;-4.;4.;5.;6.;7.|])
    // // let leftMatrix = COOMatrix(100, 100, [|0;0|], [|0;1|], [|true;true|])
    // // let rightMatrix = COOMatrix(100, 100, [|0;1|], [|0;0|], [|true;true|])
    // let leftMatrix = COOMatrix(300, 300, Array.init 500 (fun i -> 10 + i / 300), Array.init 500 (fun i -> i % 300), Array.create 500 true)
    // let rightMatrix = COOMatrix(300, 300, Array.init 600 (fun i -> i / 300), Array.init 600 (fun i -> i % 300), Array.create 600 true)

    // let workflow =
    //     opencl {
    //         let! resultMatrix = leftMatrix.EWiseAdd rightMatrix None BooleanSemiring.anyAll//FloatSemiring.addMult
    //         let! tuples = resultMatrix.GetTuples()
    //         return! tuples.ToHost()
    //         //return! resultMatrix.ToHost()
    //     }

    // // let resultMatrix : COOMatrix<float> = downcast oclContext.RunSync workflow
    // // printfn "%O" resultMatrix
    // let res = oclContext.RunSync workflow

    // for i in 0 .. res.Values.Length - 1 do
    //     printf "(%A, %A, %A); " res.RowIndices.[i] res.ColumnIndices.[i] res.Values.[i]

    // // printfn "%A" res.RowIndices
    // // printfn "%A" res.ColumnIndices
    // // printfn "%A" res.Values




    // let len = 257
    // let xs = Array.create len 1
    // let command =
    //     <@
    //         fun (ndRange: _1D)
    //             (xs: int[]) ->

    //             if ndRange.GlobalID0 < len then
    //                 barrier ()
    //                 xs.[ndRange.GlobalID0] <- 42
    //     @>
    // let wf =
    //     opencl {
    //         do! RunCommand command <| fun kernelP ->
    //             let ndRange = _1D(workSize xs.Length, workGroupSize)
    //             kernelP
    //                 ndRange
    //                 xs
    //         return! ToHost xs
    //     }

    // let res = oclContext.RunSync wf
    // printfn "%A" res.[len - 1]









    // let longSide = 7
    // let shortSide = 6
    // let sumOfSides = longSide + shortSide
    // let merge =
    //         <@
    //             fun (ndRange: _1D)
    //                 (firstRowsBuffer: int[])
    //                 (firstColumnsBuffer: int[])
    //                 (firstValuesBuffer: float[])
    //                 (secondRowsBuffer: int[])
    //                 (secondColumnsBuffer: int[])
    //                 (secondValuesBuffer: float[])
    //                 (allRowsBuffer: int[])
    //                 (allColumnsBuffer: int[])
    //                 (allValuesBuffer: float[]) ->

    //                 let i = ndRange.GlobalID0

    //                 if i < sumOfSides then
    //                     let mutable beginIdxLocal = local ()
    //                     let mutable endIdxLocal = local ()
    //                     let localID = ndRange.LocalID0
    //                     if localID < 2 then
    //                         let mutable x = localID * (workGroupSize - 1) + i - 1
    //                         if x >= sumOfSides then x <- sumOfSides - 1
    //                         let diagonalNumber = x

    //                         let mutable leftEdge = diagonalNumber + 1 - shortSide
    //                         if leftEdge < 0 then leftEdge <- 0

    //                         let mutable rightEdge = longSide - 1
    //                         if rightEdge > diagonalNumber then rightEdge <- diagonalNumber

    //                         while leftEdge <= rightEdge do
    //                             let middleIdx = (leftEdge + rightEdge) / 2
    //                             let firstIndex: uint64 = ((uint64 firstRowsBuffer.[middleIdx]) <<< 32) ||| (uint64 firstColumnsBuffer.[middleIdx])
    //                             let secondIndex: uint64 = ((uint64 secondRowsBuffer.[diagonalNumber - middleIdx]) <<< 32) ||| (uint64 secondColumnsBuffer.[diagonalNumber - middleIdx])
    //                             if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

    //                         // Here localID equals either 0 or 1
    //                         if localID = 0 then beginIdxLocal <- leftEdge else endIdxLocal <- leftEdge
    //                     barrier ()

    //                     let beginIdx = beginIdxLocal
    //                     let endIdx = endIdxLocal
    //                     let firstLocalLength = endIdx - beginIdx
    //                     let mutable x = workGroupSize - firstLocalLength
    //                     if endIdx = longSide then x <- shortSide - i + localID + beginIdx
    //                     let secondLocalLength = x

    //                     //First indices are from 0 to firstLocalLength - 1 inclusive
    //                     //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
    //                     let localIndices = localArray<uint64> workGroupSize

    //                     if localID < firstLocalLength then
    //                         localIndices.[localID] <- ((uint64 firstRowsBuffer.[beginIdx + localID]) <<< 32) ||| (uint64 firstColumnsBuffer.[beginIdx + localID])
    //                     if localID < secondLocalLength then
    //                         localIndices.[firstLocalLength + localID] <- ((uint64 secondRowsBuffer.[i - beginIdx]) <<< 32) ||| (uint64 secondColumnsBuffer.[i - beginIdx])
    //                     barrier ()

    //                     let mutable leftEdge = localID + 1 - secondLocalLength
    //                     if leftEdge < 0 then leftEdge <- 0

    //                     let mutable rightEdge = firstLocalLength - 1
    //                     if rightEdge > localID then rightEdge <- localID

    //                     while leftEdge <= rightEdge do
    //                         let middleIdx = (leftEdge + rightEdge) / 2
    //                         let firstIndex = localIndices.[middleIdx]
    //                         let secondIndex = localIndices.[firstLocalLength + localID - middleIdx]
    //                         if firstIndex < secondIndex then leftEdge <- middleIdx + 1 else rightEdge <- middleIdx - 1

    //                     let boundaryX = rightEdge
    //                     let boundaryY = localID - leftEdge

    //                     // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
    //                     let isValidX = boundaryX >= 0
    //                     let isValidY = boundaryY >= 0

    //                     let mutable fstIdx = uint64 0
    //                     if isValidX then fstIdx <- localIndices.[boundaryX]

    //                     let mutable sndIdx = uint64 0
    //                     if isValidY then sndIdx <- localIndices.[firstLocalLength + boundaryY]

    //                     if not isValidX || isValidY && fstIdx < sndIdx then
    //                         allRowsBuffer.[i] <- int (sndIdx >>> 32)
    //                         allColumnsBuffer.[i] <- int sndIdx
    //                         allValuesBuffer.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
    //                     else
    //                         allRowsBuffer.[i] <- int (fstIdx >>> 32)
    //                         allColumnsBuffer.[i] <- int fstIdx
    //                         allValuesBuffer.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
    //         @>

    // let len = 257
    // let command =
    //     <@
    //         fun (ndRange: _1D)
    //             (xs: int[]) ->

    //             if ndRange.GlobalID0 < len then
    //                 barrier ()
    //                 xs.[ndRange.GlobalID0] <- 42
    //     @>

    // //'_1D -> int[] -> int[] -> float[] -> int[] -> int[] -> float[] -> int[] -> int[] -> float[] -> unit
    // let translate2opencl (provider: ComputeProvider) (command: Quotations.Expr<('_1D -> int[] -> unit)>) : string =
    //     let options = ComputeProvider.DefaultOptions_p
    //     let tOptions = []
    //     provider.SetCompileOptions options

    //     let kernel = System.Activator.CreateInstance<Kernel<'_1D>>()
    //     CLCodeGenerator.GenerateKernel(command, provider, kernel, tOptions) |> ignore
    //     let code = (kernel :> ICLKernel).Source.ToString()
    //     code

    // let code = translate2opencl oclContext.Provider command
    // printfn "%s" code

    0
