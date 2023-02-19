module GraphBLAS.FSharp.Tests.Backend.Matrix.Convert

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Convert.Tests"

let config = defaultConfig
let wgSize = 32

let makeTest context q formatFrom formatTo convertFun isZero (array: 'a [,]) =
    let mtx =
        createMatrixFromArray2D formatFrom array isZero

    if mtx.NNZ > 0 then
        let actual =
            let mBefore = mtx.ToDevice context
            let mAfter: ClMatrix<'a> = convertFun q HostInterop mBefore
            let res = mAfter.ToHost q
            mBefore.Dispose q
            mAfter.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected =
            createMatrixFromArray2D formatTo array isZero

        "Matrices should be equal"
        |> Expect.equal actual expected

let testFixtures formatTo =
    let getCorrectnessTestName datatype formatFrom =
        sprintf "Correctness on %s, %A to %A" datatype formatFrom formatTo

    let context = defaultContext.ClContext
    let q = defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    match formatTo with
    | COO ->
        [ let convertFun = Matrix.toCOO context wgSize

          listOfUnionCases<MatrixFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest context q formatFrom formatTo convertFun ((=) 0)
                  |> testPropertyWithConfig config (getCorrectnessTestName "int" formatFrom))

          let convertFun = Matrix.toCOO context wgSize

          listOfUnionCases<MatrixFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest context q formatFrom formatTo convertFun ((=) false)
                  |> testPropertyWithConfig config (getCorrectnessTestName "bool" formatFrom)) ]
        |> List.concat
    | CSR ->
        [ let convertFun = Matrix.toCSR context wgSize

          listOfUnionCases<MatrixFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest context q formatFrom formatTo convertFun ((=) 0)
                  |> testPropertyWithConfig config (getCorrectnessTestName "int" formatFrom))

          let convertFun = Matrix.toCSR context wgSize

          listOfUnionCases<MatrixFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest context q formatFrom formatTo convertFun ((=) false)
                  |> testPropertyWithConfig config (getCorrectnessTestName "bool" formatFrom)) ]
        |> List.concat
    | CSC ->
        [ let convertFun = Matrix.toCSC context wgSize

          listOfUnionCases<MatrixFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest context q formatFrom formatTo convertFun ((=) 0)
                  |> testPropertyWithConfig config (getCorrectnessTestName "int" formatFrom))

          let convertFun = Matrix.toCSC context wgSize

          listOfUnionCases<MatrixFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest context q formatFrom formatTo convertFun ((=) false)
                  |> testPropertyWithConfig config (getCorrectnessTestName "bool" formatFrom)) ]
        |> List.concat

let tests =
    listOfUnionCases<MatrixFormat>
    |> List.collect testFixtures
    |> testList "Convert tests"
