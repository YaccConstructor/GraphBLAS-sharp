module Backend.Vector.FillSubVector

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
        compareArrays isEqual actual.Values expected.Values  "The values array must contain the same values"
        compareArrays (=) actual.Indices expected.Indices "The index array must contain the same indices"
    | _, _ -> failwith "Copy format must be the same"




let makeTest<'a, 'b when 'a: struct and 'b: struct>
    (maskFormat: VectorFormat)
    isEqual
    (isVectorItemZero: 'a -> bool)
    (isMaskItemZero: 'b -> bool)
    (fillVector: MailboxProcessor<Brahma.FSharp.Msg> -> ClVector<'a> -> ClVector<'b> -> 'a -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (array: 'a [])
    (mask: 'b [])
    (value: 'a)
    =

    if array.Length > 0 then

        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let sourceVector =
            createVectorFromArray case.FormatCase array isVectorItemZero

        let clSourceVector =
            sourceVector.ToDevice context

        let maskVector =
            createVectorFromArray maskFormat mask isMaskItemZero

        let clMaskVector =
            maskVector.ToDevice context

        let expected =
            let expected = array

            for i in 0 .. mask.Length do
                if i < array.Length && not (isMaskItemZero mask[i]) then
                    expected[i] <- value

            createVectorFromArray case.FormatCase expected isVectorItemZero

        let clActual =
            fillVector q clSourceVector clMaskVector value

        let actual = clActual.ToHost q

        clSourceVector.Dispose q
        clMaskVector.Dispose q
        clActual.Dispose q

        checkResult isEqual actual expected


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
//       |> correctnessGenericTest<int> (=) isZero intFill
//       |> testPropertyWithConfig config (getCorrectnessTestName "int")
//
//        ]
//
// let tests =
//      testCases
//     |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
//     |> List.collect testFixtures
//     |> testList "Backend.Vector.copy tests"
