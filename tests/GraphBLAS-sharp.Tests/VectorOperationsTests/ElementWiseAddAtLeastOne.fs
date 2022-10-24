module Backend.Vector.ElementWiseAddAtLeastOne

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "Vector.zeroCreate.Tests"

let clContext = defaultContext.ClContext

let checkResult
    (isEqual: 'c -> 'c -> bool)
    (zero: 'c)
    (op: AtLeastOne<'a, 'b> -> 'c option)
    (actual: Vector<'c>)
    (leftArray: 'a [])
    (rightArray: 'b []) =

    let resultExpectedLength = max leftArray.Length rightArray.Length

    "The size should be the same"
    |> Expect.equal actual.Size resultExpectedLength

    let getValueOreZero = function
        | Some value -> value
        | None -> zero

    let isLeftLess = leftArray.Length < rightArray.Length

    let lowBound =
        if isLeftLess then leftArray.Length else rightArray.Length

    let expectedArray = Array.create resultExpectedLength zero

    for i in 0 .. resultExpectedLength - 1 do
        let result =
            if i < lowBound then
                Both (leftArray[i], rightArray[i])
                |> op
                |> getValueOreZero

            elif isLeftLess then
                Left leftArray[i]
                |> op
                |> getValueOreZero
            else
                Right rightArray[i]
                |> op
                |> getValueOreZero

        expectedArray[i] <- result

    match actual with
    | VectorCOO actual ->
        let actualArray = Array.create actual.Values.Length zero

        for i in 0 .. actual.Indices.Length - 1 do
            if isEqual actual.Values[i] zero then
                failwith "Resulting zeroes should be filtered."

            actualArray[actual.Indices[i]] <- actual.Values[i]

        "arrays must have the same values"
        |> compareArrays isEqual actualArray expectedArray
    | _ -> failwith "Vector format must be the COO"

let correctnessGenericTest
    firstIsEqual
    secondIsEqual
    thirdIsEqual
    (firstZero: 'a)
    (secondZero: 'b)
    (thirdZero: 'c)
    (op: AtLeastOne<'a, 'b> -> 'c option)
    (addFun: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'b> -> ClVector<'c>) //TODO()
    (toCoo: MailboxProcessor<_> -> ClVector<'c> -> ClVector<'c>)
    case
    (leftArray: 'a [])
    (rightArray: 'b [])
    =

    if leftArray.Length > 0 && rightArray.Length > 0 then

        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let firstVector =
            createVectorFromArray case.FormatCase leftArray (firstIsEqual firstZero)

        let secondVector =
            createVectorFromArray case.FormatCase rightArray (secondIsEqual secondZero)

        let v1 = firstVector.ToDevice context
        let v2 = secondVector.ToDevice context

        let res = addFun q v1 v2

        v1.Dispose q
        v2.Dispose q

        let cooRes = toCoo q res
        res.Dispose q

        let actual = cooRes.ToHost q

        checkResult thirdIsEqual thirdZero op actual leftArray rightArray

let testFixtures (case: OperationCase<VectorFormat>) =
    let config = defaultConfig

    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, {case.FormatCase}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    let opIntSum = function
        | Both (_, x: int)
        | Left x
        | Right x ->
            Some x

    [ let addFun = Vector.elementWiseAddAtLeastOne context <@ opIntSum @>  wgSize

      let toCoo = Vector.toCoo clContext wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (opIntSum) addFun toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int") ]

let tests =
     testCases<VectorFormat>
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect testFixtures
    |> testList "Backend.Vector.copy tests"
