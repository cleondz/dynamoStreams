const uitPath           = resolveUitPath( __filename );
const createQueryStream = require( uitPath );

describe( 'QueryStream instances', ()=>{

  it( 'should invoke the query method of the supplied client instance with the supplied parameters', ()=>{

    const client = { query: sinon.spy() };
    const params = {};

    createQueryStream( client, params ).read();

    expect( client.query )
      .to.have.been.calledWith( params );

  } );

  it( 'should forward errors raised by the query', ()=>{

    const expected = new Error();
    const client   = { query: ( _, next )=>setImmediate( next, expected ) };

    createQueryStream( client, {} )
      .resume()
      .on( 'error', err=>expect( err ).to.equal( expected ) );

  } );

  it( 'should emit items returned by the query method', done=>{

    const expected = [ { foo: 'foo' } ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: expected } ) };
    const actual   = [];

    createQueryStream( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should end if no items are returned by the query method', done=>{

    const client = { query: ( _, next )=>setImmediate( next, null, {} ) };

    createQueryStream( client, {} )
      .resume()
      .on( 'end',()=>{

        done();

      } );

  } );

  it( 'should end if an empty Items array is returned by the query method', done=>{

    const client = { query: ( _, next )=>setImmediate( next, null, { Items: [] } ) };

    createQueryStream( client, {} )
      .resume()
      .on( 'end',()=>{

        done();

      } );

  } );

  it( 'should emit multiple items returned by the query method', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' } ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: expected } ) };
    const actual   = [];

    createQueryStream( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should skip nullable items returned by the query method', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' } ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: [ null, undefined, ...expected ] } ) };
    const actual   = [];

    createQueryStream( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );


  it( 'should emit all items, even if more items are returned than are read at one cycle', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' },  { baz: 'baz' }, { ban: 'ban' }   ];
    const client   = { query: ( _, next )=>setImmediate( next, null, { Items: expected } ) };
    const actual   = [];

    createQueryStream( client, {}, { highWaterMark: 1 } )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should, when a value for "LastEvaluatedKey" is returned, query for more items', done=>{

    const expected = [ { foo: 'foo' }, { bar: 'bar' } ];

    const callResults = [
      { Items: [ expected[ 0 ] ], LastEvaluatedKey: 'someKey' },
      { Items: [ expected[ 1 ] ] }
    ];

    const client = { query: ( _, next )=>setImmediate( next, null, callResults.shift() ) };
    const actual = [];

    createQueryStream( client, {} )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

  it( 'should, when a value for "LastEvaluatedKey" is returned, first emit all items from the 1st call before querying again', done=>{ // eslint-disable-line max-len

    const expected = [ { foo: 'foo' }, { bar: 'bar' },  { baz: 'baz' }, { ban: 'ban' } ];

    const callResults = [
      { Items: [ expected[ 0 ], expected[ 1 ], expected[ 2 ] ], LastEvaluatedKey: 'someKey' },
      { Items: [ expected[ 3 ] ] }
    ];

    const client = { query: ( _, next )=>setImmediate( next, null, callResults.shift() ) };
    const actual = [];

    createQueryStream( client, {}, { highWaterMark: 1 } )
      .on( 'data', actual.push.bind( actual ) )
      .on( 'end',()=>{

        expect( actual )
          .to.deep.equal( expected );

        done();

      } );

  } );

} );
