module GraphBLAS.FSharp.Tests.Backend.Vector.Convert

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger =
    Log.create "Backend.Vector.Convert.Tests"

let config = defaultConfig
let wgSize = 32

let makeTest
    formatFrom
    (convertFun: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    isZero
    case
    (array: 'a [])
    =

    let vector =
        createVectorFromArray formatFrom array isZero

    if vector.NNZ > 0 then

        let context = case.TestContext.ClContext
        let q = case.TestContext.Queue

        let actual =
            let clVector = vector.ToDevice context
            let convertedVector = convertFun q HostInterop clVector

            let res = convertedVector.ToHost q

            clVector.Dispose q
            convertedVector.Dispose q

            res

        logger.debug (
            eventX $"Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected =
            createVectorFromArray case.Format array isZero

        Expect.equal actual expected "Vectors must be the same"

let testFixtures case =
    let getCorrectnessTestName datatype formatFrom =
        sprintf "Correctness on %s, %A -> %A" datatype formatFrom case.Format

    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    q.Error.Add(fun e -> failwithf "%A" e)

    match case.Format with
    | Sparse ->
        [ let convertFun = Vector.toSparse context wgSize

          listOfUnionCases<VectorFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest formatFrom convertFun ((=) 0) case
                  |> testPropertyWithConfig config (getCorrectnessTestName "int" formatFrom))

          let convertFun = Vector.toSparse context wgSize

          listOfUnionCases<VectorFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest formatFrom convertFun ((=) false) case
                  |> testPropertyWithConfig config (getCorrectnessTestName "bool" formatFrom)) ]
        |> List.concat
    | Dense ->
        [ let convertFun = Vector.toDense context wgSize

          listOfUnionCases<VectorFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest formatFrom convertFun ((=) 0) case
                  |> testPropertyWithConfig config (getCorrectnessTestName "int" formatFrom))

          let convertFun = Vector.toDense context wgSize

          listOfUnionCases<VectorFormat>
          |> List.map
              (fun formatFrom ->
                  makeTest formatFrom convertFun ((=) false) case
                  |> testPropertyWithConfig config (getCorrectnessTestName "bool" formatFrom)) ]
        |> List.concat

let tests =
    operationGPUTests "Backend.Vector.Convert tests" testFixtures
