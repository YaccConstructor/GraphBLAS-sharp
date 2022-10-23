module Backend.Vector.Copy

open Expecto
open Expecto.Logging

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Vector.zeroCreate.Tests"

let clContext = defaultContext.ClContext

let checkResult (isEqual: 'a -> 'a -> bool) (actual: Vector<'a>) (expected: Vector<'a>) =

    Expect.equal actual.Size expected.Size "The size should be the same"

    match actual, expected with
    | VectorDense actual, VectorDense expected ->
        let isEqual left right =
            match left, right with
            | Some left, Some right ->
                isEqual left right
            | None, None -> true
            | _, _ -> false

        compareArrays isEqual actual expected "The values array must contain the default value"
    | VectorCOO actual, VectorCOO expected ->
        compareArrays isEqual actual.Values expected.Values  "The values array must contain the default value"
        compareArrays (=) actual.Indices expected.Indices "The index array must contain the 0"
    | _, _ -> failwith "Copy format must be the same"

let correctnessGenericTest<'a when 'a: struct>
    isEqual
    (isZero: 'a -> bool)
    (copy: MailboxProcessor<Brahma.FSharp.Msg> -> ClVector<'a> -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (array: 'a [])
    =
    if array.Length > 0 then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let expected =
            createVectorFromArray case.FormatCase array isZero

        let clVector = expected.ToDevice context
        let clVectorCopy = copy q clVector
        let actual = clVectorCopy.ToHost q

        clVector.Dispose q
        clVectorCopy.Dispose q

        checkResult isEqual actual expected

let testFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName datatype =
         sprintf "Correctness on %s, %A" datatype case.FormatCase

    let wgSize = 32
    let context = case.ClContext.ClContext

    [ let intCopy = Vector.copy context wgSize
      let isZero item = item = 0

      case
      |> correctnessGenericTest<int> (=) isZero intCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatCopy = Vector.copy context wgSize
      let isZero item = item = 0.0

      case
      |> correctnessGenericTest<float> (=) isZero floatCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let boolCopy = Vector.copy context wgSize
      let isZero item = item = true

      case
      |> correctnessGenericTest<bool> (=) isZero boolCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let floatCopy = Vector.copy context wgSize
      let isZero item = item = 0uy

      case
      |> correctnessGenericTest<byte> (=) isZero floatCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let tests =
     testCases
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixtures
    |> testList "Backend.Vector.copy tests"
