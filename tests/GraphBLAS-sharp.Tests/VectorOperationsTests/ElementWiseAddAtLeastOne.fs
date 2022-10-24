module Backend.Vector.ElementWiseAddAtLeastOne

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

        compareArrays isEqual actual expected "The values array must contain the same value"
    | VectorCOO actual, VectorCOO expected ->
        compareArrays isEqual actual.Values expected.Values  "The values array must contain the same values"
        compareArrays (=) actual.Indices expected.Indices "The index array must contain the same indices"
    | _, _ -> failwith "Copy format must be the same"

let makeTest
    isEqual
    secondVectorFormat
    (isZero: 'a -> bool)
    (addFun: MailboxProcessor<Brahma.FSharp.Msg> -> ClVector<'a> -> ClVector<'b> -> ClVector<'c>)
    case
    (leftVector: 'a [])
    (rightVector: 'a [])
    =

    if leftVector.Length > 0 && rightVector.Length > 0 then

        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let firstVector =
            createVectorFromArray case.FormatCase leftVector isZero

        let secondVector =
            createVectorFromArray secondVectorFormat rightVector isZero

        let v1 = firstVector.ToDevice context
        let v2 = secondVector.ToDevice context

        let res = addFun q v1 v2

        v1.Dispose q
        v2.Dispose q








//
//
// let testFixtures (case: OperationCase<VectorFormat>) =
//     let config = defaultConfig
//
//     let getCorrectnessTestName datatype =
//          sprintf "Correctness on %s, %A" datatype case.FormatCase
//
//     let wgSize = 32
//     let context = case.ClContext.ClContext
//
//     [ let intFill = Vector.fillSubVector context wgSize
//       let isZero item = item = 0
//
//       case
//       |> makeTest (=) isZero isZero intFill
//       |> testPropertyWithConfig config (getCorrectnessTestName "int")
//
//        ]
//
// let tests =
//      testCases
//     |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
//     |> List.collect testFixtures
//     |> testList "Backend.Vector.copy tests"
