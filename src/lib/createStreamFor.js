function createStreamFor( funcKey ){

  function createStream( client, params, options ){

    let isReading = false;
    let hasRead   = false;
    let ExclusiveStartKey;

    const rest = [];

    function read( n ){

      if( isReading ) return;

      if( rest.length ){

        isReading = true;

        pushInitialItems.call( this, rest, n );
        isReading = false;

        this.push( rest.shift() );
        return;

      }

      if( hasRead ){

        if( ExclusiveStartKey == null ) return setImmediate( this.push.bind( this, null ) );

        params            = { ...params, ExclusiveStartKey };
        ExclusiveStartKey = null;

      }

      hasRead   = true;
      isReading = true;

      client[ funcKey ]( params, ( err, result )=>{

        if( err ) return this.emit( 'error', { error: err, queryParameters: params } );
        if( !( result && result.Items && result.Items.length ) ) return setImmediate( this.push.bind( this, null ) );

        ExclusiveStartKey = result.LastEvaluatedKey;

        const items  = [ ...result.Items ];

        pushInitialItems.call( this, items, n );
        isReading = false;

        const item = items.shift();

        rest.push( ...items );
        setImmediate( this.push.bind( this, item ) );

      } );

    }

    return require( 'stream' ).Readable( { ...options, read, objectMode: true } );

  }

  return createStream;

}

module.exports = createStreamFor;

function pushInitialItems( items, n ){

  while( items.length > 1 && n-- > 1 ) push.call( this, items.shift() );

}

function push( item ){

  if( item != null ) this.push( item );

}
