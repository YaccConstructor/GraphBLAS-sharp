module Backend.Vector.OfList

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend

let logger = Log.create "Vector.zeroCreate.Tests"

let filter elements =
    List.filter
    <| (fun item -> fst item > 0)
    <| elements
    |> List.distinctBy fst



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

    let elements = filter elements

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

      let boolOfList =
        Vector.ofList context wgSize

      case
      |> correctnessGenericTest<bool> (=) boolOfList
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")


      let intOfList =
        Vector.ofList context wgSize

      case
      |> correctnessGenericTest<int> (=) intOfList
      |> testPropertyWithConfig config (getCorrectnessTestName "int")


      let byteOfList =
        Vector.ofList context wgSize

      case
      |> correctnessGenericTest<byte> (=) byteOfList
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")]

let tests =
    testCases
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixtures
    |> testList "Backend.Vector.ofList tests"
