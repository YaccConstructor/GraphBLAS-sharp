module Backend.Vector.OfList

open Brahma.FSharp
open Expecto
open Expecto.Logging

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Vector.zeroCreate.Tests"

let checkResultDense
    (isEqual: 'a -> 'a -> bool)
    (expectedValues: 'a option [])
    (actual: 'a option [])
    =

    let actualSize = actual.Length
    let expectedSize = expectedValues.Length

    Expect.equal actualSize expectedSize "lengths must be the same"

    let isEqual (left: 'a option) (right: 'a option) =
        match left, right with
        | Some left, Some right ->
            isEqual left right
        | None, None -> true
        | _, _ -> false

    compareArrays isEqual actual expectedValues "values must be the same"

let checkResultCOO
    (isEqual: 'a -> 'a -> bool)
    (expectedIndices: int [])
    (expectedValues: 'a [])
    (actual: COOVector<'a>)
    =

    let actualSize = actual.Size
    let expectedSize = expectedValues.Length

    Expect.equal actualSize expectedSize "lengths must be the same"

    compareArrays (=) actual.Indices expectedIndices "indices must be the same"

    compareArrays isEqual actual.Values expectedValues "values must be the same"


let correctnessGenericTest<'a when 'a: struct>
    (isEqual: 'a -> 'a -> bool)
    (ofList: (int * 'a) list -> MailboxProcessor<Msg> -> VectorFormat -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (elements: (int * 'a) list)
    =

    if elements.Length > 0 then

        let q = case.ClContext.Queue

        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let clActual =
            ofList elements q case.FormatCase

        let actual = clActual.ToHost q

        clActual.Dispose q

        match actual with
        | VectorDense actual ->
            let expected =
                createOptionArray elements

            checkResultDense isEqual expected actual
        | VectorCOO actual ->
            checkResultCOO isEqual indices values actual

let testFixtures (case: OperationCase<VectorFormat>) =
    [ let config = defaultConfig

      let wgSize = 32

      let context = case.ClContext.ClContext

      let getCorrectnessTestName datatype =
         sprintf "Correctness on %s, %A" datatype case.FormatCase

      let intOfList =
        Vector.ofList context wgSize

      case
      |> correctnessGenericTest<bool> (=) intOfList
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")


      let intOfList =
        Vector.ofList context wgSize

      case
      |> correctnessGenericTest<int> (=) intOfList
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let intOfList =
        Vector.ofList context wgSize

      case
      |> correctnessGenericTest<float> (=) intOfList
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let intOfList =
        Vector.ofList context wgSize

      case
      |> correctnessGenericTest<byte> (=) intOfList
      |> testPropertyWithConfig config (getCorrectnessTestName "int")]

let tests =
    testCases
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixtures
    |> testList "Backend.Vector.ofList tests"
