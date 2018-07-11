const sinonChai = require( 'sinon-chai' );
const chai      = require( 'chai' );

chai.use( sinonChai );

global.expect
  = chai.expect;

global.sinon
  = require( 'sinon' );

global.resolveUitPath
  = require( 'resolve-uit-path' )();
