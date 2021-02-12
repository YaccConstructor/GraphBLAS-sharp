open System

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp
open GraphBLAS.FSharp.Algorithms
open System.IO
open System
open MatrixBackend
open GraphBLAS.FSharp.Predefined
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<EntryPoint>]
let main argv =

    // let workGroupSize = 256
    // let workSize n =
    //     let m = n - 1
    //     m - m % workGroupSize + workGroupSize

    // let arr = Array.init 1000000 (fun _ -> 1)
    // let arrLength = arr.Length
    // let arr2 = Array.copy arr
    // let command =
    //     <@
    //         fun (ndRange: _1D)
    //             (input1: int[])
    //             (input2: int[]) ->

    //             let i = ndRange.GlobalID0
    //             if i < arrLength then
    //                 let (a, b) = (41, 42)
    //                 input1.[i] <- a
    //                 input2.[i] <- b
    //     @>
    // let binder kernelP =
    //     let ndRange = _1D(workSize arr.Length, workGroupSize)
    //     kernelP
    //         ndRange
    //         arr
    //         arr2

    let readMatrix (fileName: string) =
        use streamReader = new StreamReader(fileName)

        while streamReader.Peek() = int '%' do
            streamReader.ReadLine() |> ignore

        let matrixInfo =
            streamReader.ReadLine().Split(' ')
            |> Array.map int

        let (nrows, ncols, nnz) =
            matrixInfo.[0], matrixInfo.[1], matrixInfo.[2]

        [ 0 .. nnz - 1 ]
        |> List.map
            (fun _ ->
                streamReader.ReadLine().Split(' ')
                |> (fun line -> int line.[0], int line.[1], float line.[2]))
        |> List.toArray
        |> Array.sort
        |> Array.unzip3
        |> fun (rows, cols, values) ->
            let c f x y = f y x
            let rows = rows |> Array.map (c (-) 1)
            let cols = cols |> Array.map (c (-) 1)
            rows, cols, values, nrows, ncols

    // for i in 0 .. rows.Length - 1 do
    //     printfn "(%i, %i, %A)" rows.[i] cols.[i] values.[i]

    // let leftMatrix =
    //     COOMatrix<float>(100, 100,
    //         [|0;1;2;3;3;3;5;5;5;7|], [|0;0;2;0;1;2;7;8;9;0|], [|1.1;9.4;16.0;0.1;0.1;0.1;0.1;0.1;0.1;0.1|])
    // let rightMatrix =
    //     COOMatrix<float>(100, 100,
    //         [|0;0;2;4;4;4;4|], [|0;1;2;0;1;2;3|], [|-1.1;5.72;-6.0;0.1;0.1;0.1;0.1|])

    // let leftMatrix =
    //     COOMatrix<float>(100, 100,
    //         [|0;1;2|], [|0;0;2|], [|1.1;9.4;16.0|])
    // let rightMatrix =
    //     COOMatrix<float>(100, 100,
    //         [|0;0;2|], [|0;1;2|], [|-1.1;5.72;-6.0|])

    let rows, cols, values, nrows, ncols = readMatrix "webbase-1M.mtx"
    let leftMatrix = COOMatrix<float>(nrows, ncols, rows, cols, values)

    //let rows, cols, values, nrows, ncols = readMatrix "webbase-1M.mtx"
    let rightMatrix = COOMatrix<float>(nrows, ncols, rows, cols, values)

    let workflow =
        opencl {
            let! resultMatrix = leftMatrix.EWiseAdd rightMatrix None FloatSemiring.addMult
            return! resultMatrix.ToHost()
        }

    let resultMatrix : COOMatrix<float> = downcast oclContext.RunSync workflow

    // let rows = resultMatrix.Rows
    // let columns = resultMatrix.Columns
    // let values = resultMatrix.Values

    // for i in 0 .. values.Length - 1 do
    //     printfn "(%i, %i, %A)" rows.[i] columns.[i] values.[i]

    // for i in 0 .. values.Length / 2 - 1 do
    //     printfn "(%i, %i, %A)" rows.[i] columns.[i] values.[i]
    // printfn "========================================"
    // for i in values.Length / 2 .. values.Length - 1 do
    //     printfn "(%i, %i, %A)" rows.[i] columns.[i] values.[i]

    0
