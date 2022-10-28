module Backend.Vector.Complemented

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend.Common
open StandardOperations
let logger = Log.create "Vector.Complemented.Tests"

let testArrayFilter array isZero =
    Array.filter
    <| (fun item -> not <| isZero item)
    <| array

let checkResult
    isEqual
    zero
    (actual: Vector<'a>)
    (vector: 'a [])
    =

    let expectedArrayLength = vector.Length

    let expectedArray =
        Array.create expectedArrayLength 1

    for i in 0 .. expectedArrayLength - 1 do
        if isEqual vector[i] zero then
            expectedArray[i] <- 0

    match actual with
    | VectorCOO actual ->
        let actualArray = Array.create expectedArrayLength 0

        for i in 0 .. actual.Indices.Length - 1 do
            actualArray[actual.Indices[i]] <- 1

        $"arrays must have the same values and length"
        |> compareArrays (=) actualArray expectedArray
    | _ -> failwith "Vector format must be COO."

let correctnessGenericTest
    isEqual
    zero
    (complemented: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    case
    (maskArray: 'a [])
    =

    let rightFilteredArray = testArrayFilter maskArray (isEqual zero)

    if rightFilteredArray.Length > 0 then
        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let secondVector =
            createVectorFromArray case.FormatCase maskArray (isEqual zero)

        let clVector = secondVector.ToDevice context

        let res = complemented q clVector

        clVector.Dispose q

        let cooRes = toCoo q res

        res.Dispose q

        let actual = cooRes.ToHost q

        cooRes.Dispose q

        checkResult isEqual zero actual maskArray

let addTestFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.FormatCase}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    [ let intToCoo = Vector.toCoo context wgSize

      let intComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) 0 intComplemented intToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteToCoo = Vector.toCoo context wgSize

      let byteComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) 0uy byteComplemented byteToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let floatToCoo = Vector.toCoo context wgSize

      let floatComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) 0.0 floatComplemented floatToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let boolToCoo = Vector.toCoo context wgSize

      let boolComplemented = Vector.complemented context wgSize

      case
      |> correctnessGenericTest (=) false boolComplemented boolToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool") ]

let tests =
    testCases<VectorFormat>
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect addTestFixtures
    |> testList "Backend.Vector.Complemented tests"
